from operations.schemas.bucket_schemas import (
    DBLogicalBucket,
    DBPhysicalBucketLocator,
    DeleteBucketRequest,
    DeleteBucketResponse,
    DeleteBucketIsCompleted,
    LocateBucketResponse,
)
from datetime import datetime
from sqlalchemy.orm import joinedload
from fastapi import Response
from sqlalchemy import select
from operations.utils.conf import Status
from sqlalchemy.orm import Session
from fastapi import APIRouter, Depends
from operations.utils.db import get_session, logger

router = APIRouter()


@router.post("/start_delete_bucket")
async def start_delete_bucket(
    request: DeleteBucketRequest, db: Session = Depends(get_session)
) -> DeleteBucketResponse:
    logical_bucket_stmt = (
        select(DBLogicalBucket)
        .options(joinedload(DBLogicalBucket.logical_objects))
        .options(joinedload(DBLogicalBucket.physical_bucket_locators))
        .where(DBLogicalBucket.bucket == request.bucket)
    )
    logical_bucket = await db.scalar(logical_bucket_stmt)

    # print all logical buckets
    print(f"logical_bucket_stmt: {logical_bucket_stmt}")

    if logical_bucket is None:
        return Response(status_code=404, content="Bucket not found")

    if logical_bucket.status not in Status.ready or logical_bucket.logical_objects:
        return Response(
            status_code=409,
            content="Bucket is not ready for deletion, or has objects in it",
        )

    locators = []
    for locator in logical_bucket.physical_bucket_locators:
        if locator.status not in Status.ready:
            logger.error(
                f"Cannot delete physical bucket. Current status is {locator.status}"
            )
            return Response(
                status_code=409,
                content="Cannot delete physical bucket in current state",
            )

        locator.status = Status.pending_deletion
        locator.lock_acquired_ts = datetime.utcnow()
        locators.append(
            LocateBucketResponse(
                id=locator.id,
                tag=locator.location_tag,
                cloud=locator.cloud,
                bucket=locator.bucket,
                region=locator.region,
            )
        )

    logical_bucket.status = Status.pending_deletion

    try:
        await db.commit()
    except Exception as e:
        logger.error(f"Error occurred while committing changes: {e}")
        return Response(status_code=500, content="Error committing changes")

    logger.debug(f"start_delete_bucket: {request} -> {logical_bucket}")

    return DeleteBucketResponse(locators=locators)


@router.patch("/complete_delete_bucket")
async def complete_delete_bucket(
    request: DeleteBucketIsCompleted, db: Session = Depends(get_session)
):
    # TODO: need to deal with partial failures
    physical_locator_stmt = select(DBPhysicalBucketLocator).where(
        DBPhysicalBucketLocator.id == request.id
    )
    physical_locator = await db.scalar(physical_locator_stmt)

    if physical_locator is None:
        logger.error(f"physical locator not found: {request}")
        return Response(status_code=404, content="Physical Bucket Not Found")

    await db.refresh(physical_locator, ["logical_bucket"])

    logger.debug(f"complete_delete_bucket: {request} -> {physical_locator}")

    if physical_locator.status != Status.pending_deletion:
        return Response(
            status_code=409, content="Physical bucket is not marked for deletion"
        )

    # Delete the physical locator
    await db.delete(physical_locator)

    # Check if there are any remaining physical locators for the logical bucket
    remaining_physical_locators_stmt = select(DBPhysicalBucketLocator).where(
        DBPhysicalBucketLocator.logical_bucket_id == physical_locator.logical_bucket.id
    )
    remaining_physical_locators = await db.execute(remaining_physical_locators_stmt)
    if not remaining_physical_locators.all():
        await db.delete(physical_locator.logical_bucket)

    try:
        await db.commit()
    except Exception as e:
        logger.error(f"Error occurred while committing changes: {e}")
        return Response(status_code=500, content="Error committing changes")
