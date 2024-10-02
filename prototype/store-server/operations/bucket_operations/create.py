from operations.schemas.bucket_schemas import (
    DBLogicalBucket,
    DBPhysicalBucketLocator,
    RegisterBucketRequest,
    CreateBucketRequest,
    CreateBucketResponse,
    CreateBucketIsCompleted,
    LocateBucketResponse,
)
from datetime import datetime
from fastapi import Response
from sqlalchemy import select
from operations.utils.conf import Status
from sqlalchemy.orm import Session
from fastapi import APIRouter, Depends
from operations.utils.db import get_session, logger
from operations.utils.helper import init_region_tags, skystore_bucket_prefix

router = APIRouter()


@router.post("/register_buckets")
async def register_buckets(
    request: RegisterBucketRequest, db: Session = Depends(get_session)
) -> Response:
    stmt = select(DBLogicalBucket).where(DBLogicalBucket.bucket == request.bucket)
    existing_logical_bucket = await db.scalar(stmt)

    if existing_logical_bucket:
        logger.error("Bucket with this name already exists")
        return Response(status_code=409, content="Conflict, bucket already exists")

    logical_bucket = DBLogicalBucket(
        bucket=request.bucket,
        prefix="",  # TODO: integrate prefix
        status=Status.ready,
        creation_date=datetime.utcnow(),
        version_enabled=request.versioning,
    )
    db.add(logical_bucket)

    added_loc_tags = set()

    for location in request.config.physical_locations:
        physical_bucket_locator = DBPhysicalBucketLocator(
            logical_bucket=logical_bucket,
            location_tag=location.name,
            cloud=location.cloud,
            region=location.region,
            bucket=location.bucket,
            prefix="",
            lock_acquired_ts=None,
            status=Status.ready,
            is_primary=location.is_primary,
            need_warmup=location.need_warmup,
        )
        db.add(physical_bucket_locator)
        added_loc_tags.add(location.name)

    for location_tag in init_region_tags:
        if location_tag not in added_loc_tags:
            cloud, region = location_tag.split(":")
            physical_bucket_locator = DBPhysicalBucketLocator(
                logical_bucket=logical_bucket,
                location_tag=cloud + ":" + region,
                cloud=cloud,
                region=region,
                bucket=f"{skystore_bucket_prefix}-{region}",
                prefix="",
                lock_acquired_ts=None,
                status=Status.ready,
                is_primary=False,
                need_warmup=False,
            )
            db.add(physical_bucket_locator)

    await db.commit()

    return Response(
        status_code=200,
        content="Logical bucket and physical locations have been registered",
    )


@router.post("/start_create_bucket")
async def start_create_bucket(
    request: CreateBucketRequest, db: Session = Depends(get_session)
) -> CreateBucketResponse:
    stmt = select(DBLogicalBucket).where(DBLogicalBucket.bucket == request.bucket)
    existing_logical_bucket = await db.scalar(stmt)

    if existing_logical_bucket:
        logger.error("Bucket with this name already exists")
        return Response(status_code=409, content="Conflict, bucket already exists")

    logical_bucket = DBLogicalBucket(
        bucket=request.bucket,
        prefix="",
        status=Status.pending,
        creation_date=datetime.now(),
    )
    db.add(logical_bucket)

    # warmup_regions: regions to upload warmup objects to upon writes
    warmup_regions = request.warmup_regions if request.warmup_regions else []
    upload_to_region_tags = list(
        set(init_region_tags + [request.client_from_region] + warmup_regions)
    )

    bucket_locators = []

    for region_tag in upload_to_region_tags:
        cloud, region = region_tag.split(":")
        physical_bucket_name = f"{skystore_bucket_prefix}-{region}"

        bucket_locator = DBPhysicalBucketLocator(
            logical_bucket=logical_bucket,
            location_tag=region_tag,
            cloud=cloud,
            region=region,
            bucket=physical_bucket_name,
            prefix="",
            lock_acquired_ts=datetime.now(),
            status=Status.pending,
            is_primary=(region_tag == request.client_from_region),
        )
        bucket_locators.append(bucket_locator)

    db.add_all(bucket_locators)
    await db.commit()
    logger.debug(f"start_create_bucket: {request} -> {bucket_locators}")

    return CreateBucketResponse(
        locators=[
            LocateBucketResponse(
                id=locator.id,
                tag=locator.location_tag,
                cloud=locator.cloud,
                bucket=locator.bucket,
                region=locator.region,
            )
            for locator in bucket_locators
        ],
    )


@router.patch("/complete_create_bucket")
async def complete_create_bucket(
    request: CreateBucketIsCompleted, db: Session = Depends(get_session)
):
    stmt = select(DBPhysicalBucketLocator).where(
        DBPhysicalBucketLocator.id == request.id
    )
    physical_locator = await db.scalar(stmt)
    if physical_locator is None:
        logger.error(f"physical locator not found: {request}")
        return Response(status_code=404, content="Physical Bucket Not Found")
    await db.refresh(physical_locator, ["logical_bucket"])

    logger.debug(f"complete_create_bucket: {request} -> {physical_locator}")

    physical_locator.status = Status.ready
    physical_locator.lock_acquired_ts = None
    if physical_locator.is_primary:
        physical_locator.logical_bucket.status = Status.ready
        physical_locator.logical_bucket.creation_date = request.creation_date.replace(
            tzinfo=None
        )

    await db.commit()
