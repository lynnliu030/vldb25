from operations.schemas.object_schemas import (
    CleanObjectRequest,
    DBLogicalObject,
    DBPhysicalObjectLocator,
    LocateObjectRequest,
    LocateObjectResponse,
)
from operations.schemas.bucket_schemas import DBLogicalBucket
from sqlalchemy.sql import select
from sqlalchemy import and_, or_, text
from operations.utils.conf import Status
from fastapi import APIRouter, Response, Depends, status
from operations.utils.db import get_session
from operations.policy.transfer_policy.get_transfer import get_transfer_policy
from operations.policy.placement_policy.get_placement import get_placement_policy
from operations.utils.helper import policy_ultra_dict, init_region_tags, TraceIdx
from datetime import datetime, timedelta, timezone
from .clean import clean_object
from sqlalchemy.ext.asyncio import AsyncSession
import asyncio
from fastapi import BackgroundTasks

router = APIRouter()


def round_to_next_hour(timestamp: datetime) -> datetime:
    if timestamp.minute == 0 and timestamp.second == 0 and timestamp.microsecond == 0:
        return timestamp  # Already on the hour
    return (timestamp + timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)


@router.post(
    "/locate_object",
    responses={
        status.HTTP_200_OK: {"model": LocateObjectResponse},
        status.HTTP_404_NOT_FOUND: {"description": "Object not found"},
    },
)
async def locate_object(
    request: LocateObjectRequest,
    background_tasks: BackgroundTasks,
    db: AsyncSession = Depends(get_session),
) -> LocateObjectResponse:
    """Given the logical object information, return one or zero physical object locators."""

    put_policy = get_placement_policy(policy_ultra_dict["put_policy"], init_region_tags)
    get_policy = get_transfer_policy(policy_ultra_dict["get_policy"])
    is_skystore_policy = put_policy.name() == "skystore"
    is_always_store_policy = put_policy.name() == "always_store"
    is_always_evict_policy = put_policy.name() == "always_evict"

    version_enabled = (
        await db.execute(
            select(DBLogicalBucket.version_enabled).where(
                DBLogicalBucket.bucket == request.bucket
            )
        )
    ).all()

    version_enabled = version_enabled[0][0]

    if version_enabled is None and request.version_id:
        return Response(status_code=400, content="Versioning is not enabled")

    version_id = request.version_id
    idx = TraceIdx.get_instance().get()
    timestamp = datetime.now()

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

        current_hour = timestamp.replace(minute=0, second=0, microsecond=0)
        if put_policy.previous_hour is None or current_hour > put_policy.previous_hour:
            put_policy.previous_hour = current_hour
            background_tasks.add_task(
                clean_object,
                CleanObjectRequest(
                    timestamp=current_hour.strftime("%Y-%m-%d %H:%M:%S")
                ),
                db,
            )

    stmt = (
        select(DBLogicalObject)
        .join(DBPhysicalObjectLocator)
        .where(
            and_(
                DBLogicalObject.bucket == request.bucket,
                DBLogicalObject.key == request.key,
                DBLogicalObject.status == Status.ready,
                DBPhysicalObjectLocator.status == Status.ready,
                or_(
                    DBPhysicalObjectLocator.ttl == -1,  # Check if TTL is -1
                    text(
                        f"{DBPhysicalObjectLocator.__tablename__}.storage_start_time + ({DBPhysicalObjectLocator.__tablename__}.ttl || ' seconds')::interval >= :current_timestamp"
                    ).bindparams(current_timestamp=timestamp),
                ),
            )
        )
        .order_by(None if request.version_id is not None else DBLogicalObject.id.desc())
    )

    locators = (await db.scalars(stmt)).first()

    # https://docs.aws.amazon.com/AmazonS3/latest/userguide/DeletingObjectV ersions.html
    if locators is None or (locators.delete_marker and not version_id):
        if is_skystore_policy or is_always_store_policy:
            TraceIdx.get_instance().increment()
        return Response(status_code=404, content="Object Not Found")

    # https://docs.aws.amazon.com/AmazonS3/latest/userguide/DeleteMarker.html
    if locators and locators.delete_marker and version_id:
        if is_skystore_policy or is_always_store_policy:
            TraceIdx.get_instance().increment()
        return Response(status_code=405, content="Not allowed to get a delete marker")

    await db.refresh(locators, ["physical_object_locators"])
    ready_locators = locators.physical_object_locators
    chosen_locator = get_policy.get(request, ready_locators, timestamp)

    if chosen_locator.status != Status.ready:
        if is_skystore_policy or is_always_store_policy:
            TraceIdx.get_instance().increment()
        return Response(status_code=404, content="Object locator not ready")

    base_region = chosen_locator.logical_object.base_region
    src = None
    set_ttl = None

    if len(ready_locators) > 0:
        if is_skystore_policy:
            if request.client_from_region == base_region:
                set_ttl = -1
            else:
                if idx >= len(put_policy.timestamps):
                    now_timestamp = datetime.fromtimestamp(
                        put_policy.timestamps[-1] / 1000
                        + idx
                        - len(put_policy.timestamps),
                        tz=timezone.utc,
                    ).replace(tzinfo=None)
                else:
                    now_timestamp = datetime.fromtimestamp(
                        put_policy.timestamps[idx] / 1000, tz=timezone.utc
                    ).replace(tzinfo=None)
                for physical_object_locator in ready_locators:
                    if (
                        physical_object_locator.location_tag
                        != request.client_from_region
                    ):
                        ttl = put_policy.get_ttl(
                            idx,
                            physical_object_locator.location_tag,
                            request.client_from_region,
                            request.client_from_region
                            == physical_object_locator.location_tag,
                        )
                        if (
                            src is None
                            or physical_object_locator.ttl == -1
                            or (
                                ttl < set_ttl
                                and physical_object_locator.storage_start_time
                                is not None
                                and timedelta(seconds=ttl) + now_timestamp
                                <= physical_object_locator.storage_start_time
                                + timedelta(seconds=physical_object_locator.ttl)
                            )
                        ):
                            src = physical_object_locator.location_tag
                            set_ttl = ttl
    if set_ttl is None:
        if is_always_store_policy or is_skystore_policy:
            set_ttl = -1
        elif is_always_evict_policy:
            set_ttl = 0
    dst_object_ttl = set_ttl

    response = LocateObjectResponse(
        id=chosen_locator.id,
        tag=chosen_locator.location_tag,
        cloud=chosen_locator.cloud,
        bucket=chosen_locator.bucket,
        region=chosen_locator.region,
        key=chosen_locator.key,
        size=locators.size,
        last_modified=locators.last_modified,
        etag=locators.etag,
        version_id=chosen_locator.version_id
        if version_enabled is not None
        else None, 
        version=locators.id if version_enabled is not None else None,
        ttl=dst_object_ttl,  
        version_id=(
            chosen_locator.version_id if version_enabled is not None else None
        ),  
        version=locators.id if version_enabled is not None else None,
    )

    if is_skystore_policy:
        background_tasks.add_task(
            put_policy.update_past_requests, idx, response, request.client_from_region
        )

    if chosen_locator.location_tag == request.client_from_region:
        if request.client_from_region != base_region:
            if is_skystore_policy:
                chosen_locator.ttl = (
                    timestamp - chosen_locator.storage_start_time
                ).total_seconds() + dst_object_ttl
            # Persist refreshed ttl data
            await db.commit()

        put_policy.hits += 1
    else:
        put_policy.miss += 1

    if is_skystore_policy or is_always_store_policy:
        TraceIdx.get_instance().increment()

    return response
