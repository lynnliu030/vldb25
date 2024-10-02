from operations.schemas.object_schemas import (
    DBLogicalObject,
    DBPhysicalObjectLocator,
    DBLogicalMultipartUploadPart,
    DBPhysicalMultipartUploadPart,
    PatchUploadMultipartUploadId,
    PatchUploadMultipartUploadPart,
    ContinueUploadRequest,
    ContinueUploadResponse,
    ContinueUploadPhysicalPart,
)
from operations.schemas.bucket_schemas import DBLogicalBucket
from sqlalchemy.orm import selectinload, Session, joinedload
from sqlalchemy.sql import select
from sqlalchemy import and_
from operations.utils.conf import Status
from fastapi import APIRouter, Response, Depends
from operations.utils.db import get_session, logger
from typing import List

router = APIRouter()


@router.patch("/set_multipart_id")
async def set_multipart_id(
    request: PatchUploadMultipartUploadId, db: Session = Depends(get_session)
):
    # await db.execute(text("PRAGMA journal_mode=WAL;"))
    # await db.execute(text("PRAGMA synchronous=OFF;"))
    stmt = select(DBPhysicalObjectLocator).where(
        DBPhysicalObjectLocator.id == request.id
    )
    physical_locator = await db.scalar(stmt)
    if physical_locator is None:
        logger.error(f"physical locator not found: {request}")
        return Response(status_code=404, content="Not Found")
    await db.refresh(physical_locator, ["logical_object"])

    logger.debug(f"set_multipart_id: {request} -> {physical_locator}")

    physical_locator.multipart_upload_id = request.multipart_upload_id

    await db.commit()


@router.patch("/append_part")
async def append_part(
    request: PatchUploadMultipartUploadPart, db: Session = Depends(get_session)
):
    stmt = (
        select(DBPhysicalObjectLocator)
        # .join(DBLogicalObject)
        .where(DBPhysicalObjectLocator.id == request.id).options(
            joinedload(DBPhysicalObjectLocator.logical_object)
        )
    )
    physical_locator = await db.scalar(stmt)
    if physical_locator is None:
        logger.error(f"physical locator not found: {request}")
        return Response(status_code=404, content="Not Found")
    # await db.refresh(physical_locator, ["logical_object"])

    logger.debug(f"append_part: {request} -> {physical_locator}")

    await db.refresh(physical_locator, ["multipart_upload_parts"])

    existing_physical_part = next(
        (
            part
            for part in physical_locator.multipart_upload_parts
            if part.part_number == request.part_number
        ),
        None,
    )

    if existing_physical_part:
        existing_physical_part.etag = request.etag
        existing_physical_part.size = request.size
    else:
        physical_locator.multipart_upload_parts.append(
            DBPhysicalMultipartUploadPart(
                part_number=request.part_number,
                etag=request.etag,
                size=request.size,
            )
        )

    if physical_locator.is_primary:
        await db.refresh(physical_locator.logical_object, ["multipart_upload_parts"])
        existing_logical_part = next(
            (
                part
                for part in physical_locator.logical_object.multipart_upload_parts
                if part.part_number == request.part_number
            ),
            None,
        )

        if existing_logical_part:
            existing_logical_part.etag = request.etag
            existing_logical_part.size = request.size
        else:
            physical_locator.logical_object.multipart_upload_parts.append(
                DBLogicalMultipartUploadPart(
                    part_number=request.part_number,
                    etag=request.etag,
                    size=request.size,
                )
            )

    await db.commit()


@router.post("/continue_upload")
async def continue_upload(
    request: ContinueUploadRequest, db: Session = Depends(get_session)
) -> List[ContinueUploadResponse]:
    version_enabled = (
        await db.execute(
            select(DBLogicalBucket.version_enabled).where(
                DBLogicalBucket.bucket == request.bucket
            )
        )
    ).all()[0][0]

    if version_enabled is None and request.version_id:
        return Response(status_code=400, content="Versioning is not enabled")

    # the upload_id is a unique identifer
    stmt = (
        select(DBLogicalObject)
        .where(DBLogicalObject.bucket == request.bucket)
        .where(DBLogicalObject.key == request.key)
        .where(DBLogicalObject.status == Status.pending)
        .where(DBLogicalObject.multipart_upload_id == request.multipart_upload_id)
        .order_by(DBLogicalObject.id.desc())
        .options(joinedload(DBLogicalObject.physical_object_locators))
        # .first()
    )
    locators = (await db.scalars(stmt)).first()
    if locators is None:
        return Response(status_code=404, content="Not Found")

    locators = locators.physical_object_locators

    copy_src_buckets, copy_src_keys = [], []

    # cope with upload_part_copy
    if request.copy_src_bucket is not None and request.copy_src_key is not None:
        physical_src_locators = (
            (
                await db.scalars(
                    select(DBLogicalObject)
                    .options(selectinload(DBLogicalObject.physical_object_locators))
                    .where(
                        and_(
                            DBLogicalObject.bucket == request.copy_src_bucket,
                            DBLogicalObject.key == request.copy_src_key,
                            DBLogicalObject.status == Status.ready,
                            (
                                DBLogicalObject.id == request.version_id
                                if request.version_id is not None
                                else True
                            ),
                        )
                    )
                    .order_by(
                        DBLogicalObject.id.desc()
                        if request.version_id is None
                        else None
                    )
                )
            )
            .first()
            .physical_object_locators
        )

        if len(physical_src_locators) == 0:
            return Response(status_code=404, content="Source object Not Found")

        src_tags = {locator.location_tag for locator in physical_src_locators}
        dst_tags = {locator.location_tag for locator in locators}
        if src_tags != dst_tags:
            return Response(
                status_code=404,
                content=(
                    "Source object was not found in the same region that multipart upload was initiated."
                    f" src_tags={src_tags} dst_tags={dst_tags}"
                ),
            )
        src_map = {locator.location_tag: locator for locator in physical_src_locators}
        for locator in locators:
            copy_src_buckets.append(src_map[locator.location_tag].bucket)
            copy_src_keys.append(src_map[locator.location_tag].key)

    if request.do_list_parts:
        for locator in locators:
            await db.refresh(locator, ["multipart_upload_parts"])

    logger.debug(f"continue_upload: {request} -> {locators}")

    return [
        ContinueUploadResponse(
            id=locator.id,
            tag=locator.location_tag,
            cloud=locator.cloud,
            bucket=locator.bucket,
            region=locator.region,
            key=locator.key,
            multipart_upload_id=locator.multipart_upload_id,
            # version=locator.logical_object.id if version_enabled is not None else None,
            version_id=locator.version_id,
            parts=(
                [
                    ContinueUploadPhysicalPart(
                        part_number=part.part_number,
                        etag=part.etag,
                    )
                    for part in locator.multipart_upload_parts
                ]
                if request.do_list_parts
                else None
            ),
            copy_src_bucket=(
                copy_src_buckets[i] if request.copy_src_bucket is not None else None
            ),
            copy_src_key=copy_src_keys[i] if request.copy_src_key is not None else None,
        )
        for i, locator in enumerate(locators)
    ]
