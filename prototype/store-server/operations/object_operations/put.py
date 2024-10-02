import asyncio
from operations.schemas.object_schemas import (
    DBLogicalObject,
    DBPhysicalObjectLocator,
    LocateObjectResponse,
    StartUploadRequest,
    StartUploadResponse,
    PatchUploadIsCompleted,
)
from operations.schemas.bucket_schemas import DBLogicalBucket
from sqlalchemy.orm import Session, joinedload
from sqlalchemy.sql import select
from sqlalchemy import or_, text, and_
from operations.utils.conf import Status
from fastapi import APIRouter, Response, Depends
from operations.utils.db import get_session, logger
from operations.utils.helper import create_logical_object
from datetime import datetime, timedelta, timezone
from itertools import chain
from operations.policy.placement_policy.get_placement import get_placement_policy
from fastapi import BackgroundTasks
from operations.utils.helper import init_region_tags, policy_ultra_dict, TraceIdx

router = APIRouter()


def round_to_next_hour(timestamp: datetime) -> datetime:
    if timestamp.minute == 0 and timestamp.second == 0 and timestamp.microsecond == 0:
        return timestamp  # Already on the hour
    return (timestamp + timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)


@router.post("/start_upload")
async def start_upload(
    request: StartUploadRequest,
    db: Session = Depends(get_session),
) -> StartUploadResponse:
    timestamp = datetime.now()
    put_policy = get_placement_policy(policy_ultra_dict["put_policy"], init_region_tags)
    is_skystore_policy = put_policy.name() == "skystore"
    idx = TraceIdx.get_instance().get() - 1
    req_version_id = request.version_id

    if is_skystore_policy:
        if idx >= len(put_policy.timestamps):
            timestamp = datetime.fromtimestamp(
                put_policy.timestamps[-1] / 1000 + idx - len(put_policy.timestamps),
                tz=timezone.utc,
            ).replace(tzinfo=None)
        else:
            timestamp = datetime.fromtimestamp(
                put_policy.timestamps[idx] / 1000, tz=timezone.utc
            ).replace(tzinfo=None)

    res = (
        (
            await db.execute(
                select(DBLogicalBucket.version_enabled, DBLogicalBucket)
                .where(DBLogicalBucket.bucket == request.bucket)
                .options(joinedload(DBLogicalBucket.physical_bucket_locators))
            )
        )
        .unique()
        .one_or_none()
    )
    if res is None:
        return Response(status_code=404, content="Bucket Not Found")

    version_enabled, logical_bucket = res

    if not version_enabled:
        await db.execute(text("LOCK TABLE logical_objects IN EXCLUSIVE MODE;"))
        if req_version_id:
            return Response(
                status_code=400,
                content="Versioning is NULL, make sure you enable versioning first.",
            )

    # Check if the object already exists
    existing_objects_stmt = (
        select(DBLogicalObject)
        .where(
            and_(
                DBLogicalObject.bucket == request.bucket,
                DBLogicalObject.key == request.key,
                or_(
                    DBLogicalObject.status == Status.ready,
                    DBLogicalObject.status == Status.pending,
                ),
                (
                    DBLogicalObject.id == req_version_id
                    if req_version_id is not None
                    else True
                ),
            )
        )
        # Get the latest version if version_id is not specified
        .order_by(DBLogicalObject.id.desc() if req_version_id is None else None)
        .options(joinedload(DBLogicalObject.physical_object_locators))
    )
    # Get the latest version if version_id is not specified
    existing_object = (await db.scalars(existing_objects_stmt)).unique().first()

    # If version specified, and this is copy or pull-on-read operation, 404 if src object does not exist
    if (
        req_version_id
        and not existing_object
        and (
            request.copy_src_bucket is not None
            or put_policy.name() == "always_store"
            or put_policy.name() == "skystore"
        )
    ):
        return Response(
            status_code=404,
            content="Object of version {} Not Found".format(req_version_id),
        )

    primary_exists = False
    existing_tags = ()
    logical_object = None

    if existing_object:
        object_already_exists = False

        for locator in existing_object.physical_object_locators:
            if locator.location_tag == request.client_from_region:
                if locator.ttl == -1 or (
                    locator.storage_start_time is not None
                    and timestamp
                    <= (locator.storage_start_time + timedelta(seconds=locator.ttl))
                ):
                    object_already_exists = True
                else:
                    put_policy.add_to_cost(
                        locator.ttl, locator.location_tag, locator.logical_object.size
                    )

        if object_already_exists and not version_enabled:
            return Response(status_code=409, content="Conflict, object already exists")

        existing_tags = {
            locator.location_tag: locator.id
            for locator in existing_object.physical_object_locators
            if (
                locator.ttl == -1
                or (
                    locator.storage_start_time is not None
                    and timestamp
                    <= (locator.storage_start_time + timedelta(seconds=locator.ttl))
                )
            )
        }
        primary_exists = any(
            locator.is_primary for locator in existing_object.physical_object_locators
        )

        logical_object = existing_object
        logical_object.delete_marker = False

        if (
            put_policy.name() == "always_store"
            or put_policy.name() == "skystore"
            or version_enabled is None
            or not version_enabled
            or existing_object.version_suspended
        ):
            logical_object = existing_object
            logical_object.delete_marker = False
        else:
            logical_object = create_logical_object(
                existing_object, request, version_suspended=(not version_enabled)
            )
            db.add(logical_object)
    else:
        logical_object = create_logical_object(
            existing_object, request, version_suspended=(not version_enabled)
        )
        logical_object.base_region = request.client_from_region
        db.add(logical_object)

    physical_bucket_locators = logical_bucket.physical_bucket_locators
    primary_write_region = None
    upload_to_region_tags = put_policy.place(request)

    if primary_exists and (
        put_policy.name() == "always_store" or put_policy.name() == "skystore"
    ):
        # Assume that physical bucket locators for this region already exists and we don't need to create them
        primary_write_region = [
            locator.location_tag
            for locator in existing_object.physical_object_locators
            if locator.is_primary
        ]
        primary_write_region = primary_write_region[0]

    # Push-based: upload to primary region and broadcast to other regions marked with need_warmup
    elif put_policy.name() == "push" or put_policy.name() == "replicate_all":
        # Except this case, always set the first-write region of the OBJECT to be primary
        primary_write_region = [
            locator.location_tag
            for locator in physical_bucket_locators
            if locator.is_primary
        ]
        assert (
            len(primary_write_region) == 1
        ), "should only have one primary write region"
        primary_write_region = primary_write_region[0]
    elif put_policy.name() == "single_region":
        primary_write_region = upload_to_region_tags[0]
    else:
        primary_write_region = request.client_from_region

    set_ttl = (
        request.ttl
        if request.ttl is not None
        else put_policy.get_ttl(idx, dst=primary_write_region, fixed_base_region=True)
    )

    # If a copy operation
    copy_src_buckets = []
    copy_src_keys = []
    if (request.copy_src_bucket is not None) and (request.copy_src_key is not None):
        copy_src_stmt = (
            select(DBLogicalObject)
            .where(
                and_(
                    DBLogicalObject.bucket == request.copy_src_bucket,
                    DBLogicalObject.key == request.copy_src_key,
                    DBLogicalObject.status == Status.ready,
                    (
                        DBLogicalObject.id
                        == req_version_id  # copy the specific version if specified
                        if req_version_id is not None
                        else True
                    ),
                )
            )
            .order_by(DBLogicalObject.id.desc() if req_version_id is None else None)
            .options(joinedload(DBLogicalObject.physical_object_locators))
        )

        copy_src_locator = (await db.scalars(copy_src_stmt)).unique().first()

        # https://docs.aws.amazon.com/AmazonS3/latest/userguide/DeletingObjectVersions.html
        if copy_src_locator is None or (
            copy_src_locator.delete_marker and not req_version_id
        ):
            return Response(status_code=404, content="Object Not Found")

        # https://docs.aws.amazon.com/cli/latest/reference/s3api/upload-part-copy.html
        if copy_src_locator and copy_src_locator.delete_marker and req_version_id:
            return Response(
                status_code=400, content="Not allowed to copy from a delete marker"
            )

        copy_src_locators_map = {
            locator.location_tag: locator
            for locator in copy_src_locator.physical_object_locators
        }
        copy_src_locations = set(
            locator.location_tag
            for locator in copy_src_locator.physical_object_locators
        )
    else:
        copy_src_locations = None

    # Only copy where the source object exists.
    if copy_src_locations is not None:
        upload_to_region_tags = [
            tag for tag in upload_to_region_tags if tag in copy_src_locations
        ]
        if len(upload_to_region_tags) == 0:
            upload_to_region_tags = list(copy_src_locations)

        copy_src_buckets = [
            copy_src_locators_map[tag].bucket for tag in copy_src_locations
        ]
        copy_src_keys = [copy_src_locators_map[tag].key for tag in copy_src_locations]

        logger.debug(
            f"start_upload: copy_src_locations={copy_src_locations}, "
            f"upload_to_region_tags={upload_to_region_tags}, "
            f"copy_src_buckets={copy_src_buckets}, "
            f"copy_src_keys={copy_src_keys}"
        )

    locators, existing_locators = [], []

    for region_tag in upload_to_region_tags:
        if region_tag in existing_tags and version_enabled is None:
            continue

        physical_bucket_locator = next(
            (pbl for pbl in physical_bucket_locators if pbl.location_tag == region_tag),
            None,
        )
        if physical_bucket_locator is None:
            logger.error(
                f"No physical bucket locator found for region tag: {region_tag}"
            )
            return Response(
                status_code=500,
                content=f"No physical bucket locator found for upload region tag {region_tag}",
            )

        if (
            region_tag not in existing_tags
            or version_enabled
            or (existing_object and existing_object.version_suspended is False)
        ):
            locators.append(
                DBPhysicalObjectLocator(
                    logical_object=logical_object,
                    location_tag=region_tag,
                    cloud=physical_bucket_locator.cloud,
                    region=physical_bucket_locator.region,
                    bucket=physical_bucket_locator.bucket,
                    key=physical_bucket_locator.prefix + request.key,
                    lock_acquired_ts=timestamp,
                    status=Status.pending,
                    is_primary=(region_tag == primary_write_region),
                    ttl=set_ttl,
                )
            )
        else:
            existing_locators.append(
                DBPhysicalObjectLocator(
                    id=existing_tags[region_tag],
                    logical_object=logical_object,
                    location_tag=region_tag,
                    cloud=physical_bucket_locator.cloud,
                    region=physical_bucket_locator.region,
                    bucket=physical_bucket_locator.bucket,
                    key=physical_bucket_locator.prefix + request.key,
                    lock_acquired_ts=timestamp,
                    status=Status.pending,
                    is_primary=(region_tag == primary_write_region),
                    ttl=None,
                )
            )

    db.add_all(locators)
    await db.commit()

    return StartUploadResponse(
        multipart_upload_id=logical_object.multipart_upload_id,
        locators=[
            LocateObjectResponse(
                id=locator.id,
                tag=locator.location_tag,
                cloud=locator.cloud,
                bucket=locator.bucket,
                region=locator.region,
                key=locator.key,
                version_id=locator.version_id,
                version=(
                    locator.logical_object.id if version_enabled is not None else None
                ),
                ttl=locator.ttl,
            )
            for locator in chain(locators, existing_locators)
        ],
        copy_src_buckets=copy_src_buckets,
        copy_src_keys=copy_src_keys,
    )


@router.patch("/complete_upload")
async def complete_upload(
    request: PatchUploadIsCompleted, db: Session = Depends(get_session)
):
    put_policy = get_placement_policy(policy_ultra_dict["put_policy"], init_region_tags)
    is_skystore_policy = put_policy.name() == "skystore"

    stmt = (
        select(DBPhysicalObjectLocator)
        .where(DBPhysicalObjectLocator.id == request.id)
        .options(joinedload(DBPhysicalObjectLocator.logical_object))
    )
    physical_locator = await db.scalar(stmt)
    if physical_locator is None:
        logger.error(f"physical locator not found: {request}")
        return Response(status_code=404, content="Not Found")

    physical_locator.status = Status.ready
    physical_locator.lock_acquired_ts = None
    physical_locator.version_id = request.version_id

    if physical_locator.storage_start_time is None:
        idx = TraceIdx.get_instance().get() - 1
        timestamp = request.last_modified.replace(tzinfo=None)
        if is_skystore_policy:
            if idx >= len(put_policy.timestamps):
                timestamp = datetime.fromtimestamp(
                    put_policy.timestamps[-1] / 1000 + idx - len(put_policy.timestamps),
                    tz=timezone.utc,
                ).replace(tzinfo=None)
            else:
                timestamp = datetime.fromtimestamp(
                    put_policy.timestamps[idx] / 1000, tz=timezone.utc
                ).replace(tzinfo=None)
        physical_locator.storage_start_time = timestamp

    if request.ttl is not None:
        physical_locator.ttl = request.ttl

    policy_name = put_policy.name()
    if (
        (
            (policy_name == "push" or policy_name == "replicate_all")
            and physical_locator.is_primary
        )
        or policy_name == "always_store"
        or policy_name == "always_evict"
        or policy_name == "single_region"
        or policy_name == "fixed_ttl"
        or policy_name == "t_even"
        or policy_name == "skystore"
    ):
        logical_object = physical_locator.logical_object
        logical_object.status = Status.ready
        logical_object.size = request.size
        logical_object.etag = request.etag
        logical_object.last_modified = request.last_modified.replace(tzinfo=None)
    await db.commit()
