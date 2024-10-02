from operations.schemas.object_schemas import (
    DBLogicalObject,
    DBPhysicalObjectLocator,
    LocateObjectResponse,
    StartWarmupRequest,
    StartWarmupResponse,
)
from operations.schemas.bucket_schemas import DBLogicalBucket
from sqlalchemy.orm import selectinload, Session
from sqlalchemy.sql import select
from operations.utils.conf import Status
from fastapi import APIRouter, Response, Depends
from operations.utils.db import get_session, logger

router = APIRouter()


@router.post("/start_warmup")
async def start_warmup(
    request: StartWarmupRequest, db: Session = Depends(get_session)
) -> StartWarmupResponse:
    """Given the logical object information and warmup regions, return one or zero physical object locators."""

    version_enabled = (
        await db.execute(
            select(DBLogicalBucket.version_enabled).where(
                DBLogicalBucket.bucket == request.bucket
            )
        )
    ).all()[0][0]

    if version_enabled is None and request.version_id:
        return Response(status_code=400, content="Versioning is not enabled")

    if request.version_id is not None:
        stmt = (
            select(DBLogicalObject)
            .options(selectinload(DBLogicalObject.physical_object_locators))
            .where(DBLogicalObject.bucket == request.bucket)
            .where(DBLogicalObject.key == request.key)
            .where(DBLogicalObject.status == Status.ready)
            .where(
                DBLogicalObject.id == request.version_id
            )  # select the one with specific version
        )
    else:
        stmt = (
            select(DBLogicalObject)
            .options(selectinload(DBLogicalObject.physical_object_locators))
            .where(DBLogicalObject.bucket == request.bucket)
            .where(DBLogicalObject.key == request.key)
            .where(DBLogicalObject.status == Status.ready)
            .order_by(DBLogicalObject.id.desc())  # select the latest version
            # .first()
        )
    locators = (await db.scalars(stmt)).first()

    if locators is None:
        return Response(status_code=404, content="Object Not Found")

    primary_locator = None
    for physical_locator in locators.physical_object_locators:
        if physical_locator.is_primary:
            primary_locator = physical_locator
            break

    if primary_locator is None:
        logger.error("No primary locator found.")
        return Response(status_code=500, content="Internal Server Error")

    logical_bucket = (
        await db.execute(
            select(DBLogicalBucket)
            .options(selectinload(DBLogicalBucket.physical_bucket_locators))
            .where(DBLogicalBucket.bucket == request.bucket)
        )
    ).scalar_one_or_none()

    # Transfer to warmup regions
    secondary_locators = []
    for region_tag in [
        region for region in request.warmup_regions if region != primary_locator.region
    ]:
        physical_bucket_locator = next(
            (
                pbl
                for pbl in logical_bucket.physical_bucket_locators
                if pbl.location_tag == region_tag
            ),
            None,
        )
        if not physical_bucket_locator:
            logger.error(
                f"No physical bucket locator found for warmup region: {region_tag}"
            )
            return Response(
                status_code=500,
                content=f"No physical bucket locator found for warmup {region_tag}",
            )
        secondary_locator = DBPhysicalObjectLocator(
            logical_object=primary_locator.logical_object,
            location_tag=region_tag,
            cloud=physical_bucket_locator.cloud,
            region=physical_bucket_locator.region,
            bucket=physical_bucket_locator.bucket,
            key=physical_bucket_locator.prefix + request.key,
            status=Status.pending,
            is_primary=False,
            version_id=primary_locator.version_id,  # same version of the primary locator
        )
        secondary_locators.append(secondary_locator)
        db.add(secondary_locator)

    for locator in secondary_locators:
        locator.status = Status.pending

    await db.commit()
    return StartWarmupResponse(
        src_locator=LocateObjectResponse(
            id=primary_locator.id,
            tag=primary_locator.location_tag,
            cloud=primary_locator.cloud,
            bucket=primary_locator.bucket,
            region=primary_locator.region,
            key=primary_locator.key,
            version_id=primary_locator.version_id,
            version=(
                primary_locator.logical_object.id
                if version_enabled is not None
                else None
            ),
        ),
        dst_locators=[
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
                ),  # logical version
            )
            for locator in secondary_locators
        ],
    )
