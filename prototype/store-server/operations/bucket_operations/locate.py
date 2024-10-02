from operations.schemas.bucket_schemas import (
    DBLogicalBucket,
    DBPhysicalBucketLocator,
    LocateBucketRequest,
    LocateBucketResponse,
)
from fastapi import Response
from sqlalchemy import select
from operations.utils.conf import Status
from sqlalchemy.orm import Session
from fastapi import APIRouter, Depends, status
from operations.utils.db import get_session, logger

router = APIRouter()


@router.post(
    "/locate_bucket",
    responses={
        status.HTTP_200_OK: {"model": LocateBucketResponse},
        status.HTTP_404_NOT_FOUND: {"description": "Bucket not found"},
    },
)
async def locate_bucket(
    request: LocateBucketRequest, db: Session = Depends(get_session)
) -> LocateBucketResponse:
    """Given the bucket name, return one or zero physical bucket locators."""
    stmt = (
        select(DBPhysicalBucketLocator)
        .join(DBLogicalBucket)
        .where(DBLogicalBucket.bucket == request.bucket)
        .where(DBLogicalBucket.status == Status.ready)
    )
    locators = (await db.scalars(stmt)).all()

    if len(locators) == 0:
        return Response(status_code=404, content="Bucket Not Found")

    chosen_locator = None
    reason = ""
    for locator in locators:
        if locator.location_tag == request.client_from_region:
            chosen_locator = locator
            reason = "exact match"
            break
    else:
        # find the primary locator
        chosen_locator = next(locator for locator in locators if locator.is_primary)
        reason = "fallback to primary"

    logger.debug(
        f"locate_bucket: chosen locator with strategy {reason} out of {len(locators)}, {request} -> {chosen_locator}"
    )

    await db.refresh(chosen_locator, ["logical_bucket"])

    return LocateBucketResponse(
        id=chosen_locator.id,
        tag=chosen_locator.location_tag,
        cloud=chosen_locator.cloud,
        bucket=chosen_locator.bucket,
        region=chosen_locator.region,
    )
