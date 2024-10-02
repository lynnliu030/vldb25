from operations.schemas.bucket_schemas import (
    DBLogicalBucket,
    DBPhysicalBucketLocator,
    BucketResponse,
    LocateBucketRequest,
    LocateBucketResponse,
    HeadBucketRequest,
    BucketStatus,
    PutBucketVersioningRequest,
)
from fastapi import Response
from sqlalchemy import select
from operations.utils.conf import Status
from sqlalchemy.orm import Session
from fastapi import APIRouter, Depends, status
from operations.utils.db import get_session, logger
from typing import List

router = APIRouter()


@router.post("/list_buckets")
async def list_buckets(db: Session = Depends(get_session)) -> List[BucketResponse]:
    stmt = select(DBLogicalBucket).where(DBLogicalBucket.status == Status.ready)
    buckets = (await db.scalars(stmt)).all()

    logger.debug(f"list_buckets: -> {buckets}")

    return [
        BucketResponse(
            bucket=bucket.bucket,
            creation_date=bucket.creation_date,
        )
        for bucket in buckets
    ]


@router.post("/head_bucket")
async def head_bucket(request: HeadBucketRequest, db: Session = Depends(get_session)):
    stmt = select(DBLogicalBucket).where(
        DBLogicalBucket.bucket == request.bucket, DBLogicalBucket.status == Status.ready
    )
    bucket = await db.scalar(stmt)

    if bucket is None:
        return Response(status_code=404, content="Not Found")

    logger.debug(f"head_bucket: {request} -> {bucket}")

    return Response(
        status_code=200,
        content="Bucket exists",
    )


@router.post("/put_bucket_versioning")
async def put_bucket_versioning(
    request: PutBucketVersioningRequest, db: Session = Depends(get_session)
) -> List[LocateBucketResponse]:
    stmt = select(DBLogicalBucket).where(
        DBLogicalBucket.bucket == request.bucket, DBLogicalBucket.status == Status.ready
    )
    bucket = await db.scalar(stmt)

    if bucket is None:
        return Response(status_code=404, content="Not Found")

    logger.debug(f"put_bucket_versioning: {request} -> {bucket}")

    bucket.version_enabled = request.versioning
    locators_lst = []
    await db.refresh(bucket, ["physical_bucket_locators"])
    for physical_bucket_locator in bucket.physical_bucket_locators:
        locators_lst.append(
            LocateBucketResponse(
                id=physical_bucket_locator.id,
                tag=physical_bucket_locator.location_tag,
                cloud=physical_bucket_locator.cloud,
                bucket=physical_bucket_locator.bucket,
                region=physical_bucket_locator.region,
            )
        )

    await db.commit()

    return locators_lst


@router.post("/check_version_setting")
async def check_version_setting(
    request: HeadBucketRequest, db: Session = Depends(get_session)
) -> bool:
    stmt = select(DBLogicalBucket).where(
        DBLogicalBucket.bucket == request.bucket, DBLogicalBucket.status == Status.ready
    )
    bucket = await db.scalar(stmt)

    if bucket is None:
        return Response(status_code=404, content="Not Found")

    logger.debug(f"check_version_setting: {request} -> {bucket}")

    # both suspended and enabled versioning setting should be able to upload objects multiple times
    if bucket.version_enabled is None:
        return False
    return True


@router.post(
    "/locate_bucket_status",
    responses={
        status.HTTP_200_OK: {"model": BucketStatus},
        status.HTTP_404_NOT_FOUND: {"description": "Bucket not found"},
    },
)
async def locate_bucket_status(
    request: LocateBucketRequest, db: Session = Depends(get_session)
) -> BucketStatus:
    """Given the bucket name, return physical bucket status. Currently only used for testing metadata cleanup"""
    stmt = (
        select(DBPhysicalBucketLocator)
        .join(DBLogicalBucket)
        .where(DBLogicalBucket.bucket == request.bucket)
    )

    locators = (await db.scalars(stmt)).all()

    if len(locators) == 0:
        return Response(status_code=404, content="Bucket Not Found")

    chosen_locator = None
    for locator in locators:
        if locator.location_tag == request.client_from_region:
            chosen_locator = locator
            break
    else:
        chosen_locator = next(locator for locator in locators if locator.is_primary)

    await db.refresh(chosen_locator, ["logical_bucket"])

    return BucketStatus(
        status=chosen_locator.status,
    )
