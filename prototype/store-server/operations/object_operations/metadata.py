from operations.schemas.object_schemas import (
    DBLogicalObject,
    ObjectResponse,
    LocateObjectRequest,
    ObjectStatus,
    ListObjectRequest,
    HeadObjectRequest,
    HeadObjectResponse,
    ListPartsRequest,
    LogicalPartResponse,
    MultipartResponse,
    Metrics,
    DBMetrics,
)
from operations.schemas.bucket_schemas import DBLogicalBucket
from sqlalchemy.orm import selectinload, Session
from sqlalchemy.sql import select
from sqlalchemy import and_
from sqlalchemy import func
from operations.utils.conf import Status
from fastapi import APIRouter, Response, Depends, status
from operations.utils.db import get_session, logger
from typing import List

router = APIRouter()


@router.post("/list_objects")
async def list_objects(
    request: ListObjectRequest, db: Session = Depends(get_session)
) -> List[ObjectResponse]:
    stmt = select(DBLogicalBucket).where(
        DBLogicalBucket.bucket == request.bucket, DBLogicalBucket.status == Status.ready
    )
    logical_bucket = await db.scalar(stmt)
    if logical_bucket is None:
        return Response(status_code=404, content="Bucket Not Found")

    conditions = [
        DBLogicalObject.bucket == logical_bucket.bucket,
        DBLogicalObject.status == Status.ready,
        DBLogicalObject.delete_marker is not True,  # Exclude delete markers
    ]

    if request.prefix is not None:
        conditions.append(DBLogicalObject.key.startswith(request.prefix))
    if request.start_after is not None:
        conditions.append(DBLogicalObject.key > request.start_after)

    subquery = (
        select(DBLogicalObject.key, func.max(DBLogicalObject.id).label("max_id"))
        .where(*conditions)
        .group_by(DBLogicalObject.key)
        .subquery()
    )

    # Create the main query that joins the subquery
    stmt = (
        select(DBLogicalObject)
        .join(
            subquery,
            and_(
                DBLogicalObject.key == subquery.c.key,
                DBLogicalObject.id == subquery.c.max_id,
            ),
        )
        .order_by(DBLogicalObject.key)
    )

    if request.max_keys is not None:
        stmt = stmt.limit(request.max_keys)

    objects = await db.execute(stmt)
    objects_all = objects.scalars().all()  # NOTE: DO NOT use `scalars` here
    logger.debug(f"list_objects: {request} -> {objects_all}")

    return [
        ObjectResponse(
            bucket=obj.bucket,
            key=obj.key,
            size=obj.size,
            etag=obj.etag,
            last_modified=obj.last_modified,
        )
        for obj in objects_all
    ]


@router.post("/list_objects_versioning")
async def list_objects_versioning(
    request: ListObjectRequest, db: Session = Depends(get_session)
) -> List[ObjectResponse]:
    stmt = select(DBLogicalBucket).where(
        DBLogicalBucket.bucket == request.bucket, DBLogicalBucket.status == Status.ready
    )
    logical_bucket = await db.scalar(stmt)
    if logical_bucket is None:
        return Response(status_code=404, content="Bucket Not Found")

    stmt = select(DBLogicalObject).where(
        DBLogicalObject.bucket == logical_bucket.bucket,
        DBLogicalObject.status == Status.ready,
    )

    if request.prefix is not None:
        stmt = stmt.where(DBLogicalObject.key.startswith(request.prefix))
    if request.start_after is not None:
        stmt = stmt.where(DBLogicalObject.key > request.start_after)

    # Sort keys before return
    stmt = stmt.order_by(DBLogicalObject.key)

    # Limit the number of returned objects if specified
    if request.max_keys is not None:
        stmt = stmt.limit(request.max_keys)

    objects = await db.scalars(stmt)
    objects_all = objects.all()

    if not objects_all:
        return []

    logger.debug(f"list_objects: {request} -> {objects_all}")

    return [
        ObjectResponse(
            bucket=obj.bucket,
            key=obj.key,
            size=obj.size,
            etag=obj.etag,
            last_modified=obj.last_modified,
            version_id=obj.id,
        )
        for obj in objects_all
    ]


@router.post("/head_object")
async def head_object(
    request: HeadObjectRequest, db: Session = Depends(get_session)
) -> HeadObjectResponse:
    version_enabled = (
        await db.execute(
            select(DBLogicalBucket.version_enabled).where(
                DBLogicalBucket.bucket == request.bucket
            )
        )
    ).all()[0][0]

    if version_enabled is None and request.version_id:
        return Response(status_code=400, content="Versioning is not enabled")

    stmt = (
        select(DBLogicalObject)
        .where(
            and_(
                DBLogicalObject.bucket == request.bucket,
                DBLogicalObject.key == request.key,
                DBLogicalObject.status == Status.ready,
                (
                    DBLogicalObject.id == request.version_id
                    if request.version_id is not None
                    else True
                ),
            )
        )
        .order_by(DBLogicalObject.id.desc() if request.version_id is None else None)
    )

    logical_object = (await db.scalars(stmt)).first()

    # https://docs.aws.amazon.com/AmazonS3/latest/userguide/DeletingObjectVersions.html
    if logical_object is None or (
        logical_object.delete_marker and not request.version_id
    ):
        return Response(status_code=404, content="Object Not Found")

    # https://docs.aws.amazon.com/AmazonS3/latest/userguide/DeleteMarker.html
    if logical_object and logical_object.delete_marker and request.version_id:
        return Response(status_code=405, content="Not allowed to get a delete marker")

    logger.debug(f"head_object: {request} -> {logical_object}")

    return HeadObjectResponse(
        bucket=logical_object.bucket,
        key=logical_object.key,
        size=logical_object.size,
        etag=logical_object.etag,
        last_modified=logical_object.last_modified,
        version_id=logical_object.id if version_enabled is not None else None,
    )


@router.post("/list_multipart_uploads")
async def list_multipart_uploads(
    request: ListObjectRequest, db: Session = Depends(get_session)
) -> List[MultipartResponse]:
    stmt = (
        select(DBLogicalObject)
        .where(DBLogicalObject.bucket == request.bucket)
        .where(DBLogicalObject.key.startswith(request.prefix))
        .where(DBLogicalObject.status == Status.pending)
    )
    objects = (await db.scalars(stmt)).all()

    logger.debug(f"list_multipart_uploads: {request} -> {objects}")

    return [
        MultipartResponse(
            bucket=obj.bucket,
            key=obj.key,
            upload_id=obj.multipart_upload_id,
        )
        for obj in objects
    ]


@router.post("/list_parts")
async def list_parts(
    request: ListPartsRequest, db: Session = Depends(get_session)
) -> List[LogicalPartResponse]:
    stmt = (
        select(DBLogicalObject)
        .where(DBLogicalObject.bucket == request.bucket)
        .where(DBLogicalObject.key == request.key)
        .where(DBLogicalObject.status == Status.pending)
        .where(DBLogicalObject.multipart_upload_id == request.upload_id)
    )
    objects = (await db.scalars(stmt)).all()
    if len(objects) == 0:
        return Response(status_code=404, content="Object Multipart Not Found")

    # assert len(objects) == 1, "should only have one object"
    await db.refresh(objects[0], ["multipart_upload_parts"])
    logger.debug(
        f"list_parts: {request} -> {objects[0], objects[0].multipart_upload_parts}"
    )

    return [
        LogicalPartResponse(
            part_number=part.part_number,
            etag=part.etag,
            size=part.size,
        )
        for obj in objects
        for part in obj.multipart_upload_parts
        if request.part_number is None or part.part_number == request.part_number
    ]

@router.post(
    "/locate_object_status",
    responses={
        status.HTTP_200_OK: {"model": ObjectStatus},
        status.HTTP_404_NOT_FOUND: {"description": "Object not found"},
    },
)
async def locate_object_status(
    request: LocateObjectRequest, db: Session = Depends(get_session)
) -> List[ObjectStatus]:
    """Given the logical object information, return the status of the object.
    Currently only used for testing metadata cleanup."""

    version_enabled = (
        await db.execute(
            select(DBLogicalBucket.version_enabled).where(
                DBLogicalBucket.bucket == request.bucket
            )
        )
    ).all()[0][0]

    if version_enabled is None and request.version_id:
        return Response(status_code=400, content="Versioning is not enabled")

    object_status_lst = []
    if request.version_id is not None:
        stmt = (
            select(DBLogicalObject)
            .options(selectinload(DBLogicalObject.physical_object_locators))
            .where(DBLogicalObject.bucket == request.bucket)
            .where(DBLogicalObject.key == request.key)
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
        )
    # get physical locators
    locators = (await db.scalars(stmt)).all()
    if len(locators) == 0:
        return Response(status_code=404, content="Object Not Found")

    chosen_locator = []
    reason = ""

    for logical_object in locators:
        await db.refresh(logical_object, ["physical_object_locators"])
        for physical_locator in logical_object.physical_object_locators:
            if physical_locator.location_tag == request.client_from_region:
                chosen_locator.append(physical_locator)
                reason = "exact match"
                break

        if len(chosen_locator) == 0:
            # find the primary locator
            for physical_locator in logical_object.physical_object_locators:
                if physical_locator.is_primary:
                    chosen_locator.append(physical_locator)
                    reason = "fallback to primary"
                    break

    logger.debug(
        f"locate_object: chosen locator with strategy {reason} out of {len(locators)}, {request} -> {chosen_locator}"
    )

    for locator in chosen_locator:
        object_status_lst.append(ObjectStatus(status=locator.status))
    # return object status
    return object_status_lst


@router.post("/update_metrics")
async def update_metrics(metric: Metrics, db: Session = Depends(get_session)):
    # build the metrics
    mtr = DBMetrics(
        timestamp=metric.timestamp,
        request_region=metric.request_region,
        destination_region=metric.destination_region,
        latency=metric.latency,
        key=metric.key,
        size=metric.size,
        op=metric.op,
    )

    db.add(mtr)
    await db.commit()
