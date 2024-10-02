from datetime import datetime, timedelta
from operations.schemas.object_schemas import (
    CleanObjectRequest,
    CleanObjectResponse,
    DBPhysicalObjectLocator,
    LocateObjectResponse,
)
from fastapi import APIRouter, Depends
from sqlalchemy import delete, select
from sqlalchemy.orm import Session, selectinload
from sqlalchemy.ext.asyncio import AsyncSession
from operations.utils.db import get_session
from operations.schemas.bucket_schemas import Status
from sqlalchemy import text
from operations.utils.helper import create_object_store_interface
from operations.policy.placement_policy.get_placement import get_placement_policy
from operations.utils.helper import policy_ultra_dict, init_region_tags
from sqlalchemy import func
from sqlalchemy import tuple_, update

router = APIRouter()
c = 0


def round_to_next_30_min(dt: datetime) -> datetime:
    minutes = dt.minute
    if minutes == 0 or minutes == 30:
        return dt.replace(second=0, microsecond=0)
    elif minutes < 30:
        rounded_dt = dt.replace(minute=30, second=0, microsecond=0)
    else:
        rounded_dt = (dt + timedelta(hours=1)).replace(
            minute=0, second=0, microsecond=0
        )

    return rounded_dt


@router.post("/clean_object")
async def clean_object(
    request: CleanObjectRequest, db: AsyncSession = Depends(get_session)
) -> CleanObjectResponse:
    """Given the current timestamp, clean the object based on TTL."""
    async with db:
        put_policy = get_placement_policy(
            policy_ultra_dict["put_policy"], init_region_tags
        )
        is_skystore = put_policy.name() == "skystore"

        subquery = (
            select(
                DBPhysicalObjectLocator.bucket,
                DBPhysicalObjectLocator.key,
                DBPhysicalObjectLocator.location_tag,
                func.max(DBPhysicalObjectLocator.id).label("max_id"),
            )
            .group_by(
                DBPhysicalObjectLocator.bucket,
                DBPhysicalObjectLocator.key,
                DBPhysicalObjectLocator.location_tag,
            )
            .subquery()
        )

        # Select the objects with the highest ID for each bucket, key, and location_tag
        highest_id_objects_query = (
            select(DBPhysicalObjectLocator)
            .join(subquery, DBPhysicalObjectLocator.id == subquery.c.max_id)
            .options(selectinload(DBPhysicalObjectLocator.logical_object))
        )

        highest_id_objects = await db.execute(highest_id_objects_query)
        highest_id_objects = highest_id_objects.scalars().all()
        objects_to_delete = [
            o
            for o in highest_id_objects
            if o.status == Status.ready
            and o.ttl != -1
            and o.storage_start_time + timedelta(seconds=o.ttl) < request.timestamp
        ]

        if not objects_to_delete:
            return CleanObjectResponse(locators=[])

        global c
        if is_skystore:
            for object in objects_to_delete:
                c += 1
                put_policy.add_to_cost(
                    (
                        round_to_next_30_min(
                            object.storage_start_time + timedelta(seconds=object.ttl)
                        )
                        - object.storage_start_time
                    ).total_seconds(),
                    object.location_tag,
                    object.logical_object.size,
                )

        delete_conditions = [
            (obj.bucket, obj.key, obj.location_tag) for obj in objects_to_delete
        ]

        try:
            await db.execute(
                update(DBPhysicalObjectLocator)
                .where(
                    tuple_(
                        DBPhysicalObjectLocator.bucket,
                        DBPhysicalObjectLocator.key,
                        DBPhysicalObjectLocator.location_tag,
                    ).in_(delete_conditions)
                )
                .values(status=Status.pending)  # Update status to PENDING
            )
            await db.commit()
        except Exception as e:
            print(
                f"Failed to update objects to status PENDING in database: {e}, rolling back"
            )
            await db.rollback()
            return CleanObjectResponse(locators=[])

        locators_by_tag = {}
        for obj in objects_to_delete:
            locators_by_tag.setdefault(obj.location_tag, []).append(obj)

        locators_to_interface = {
            tag: create_object_store_interface(tag) for tag in locators_by_tag
        }

        for tag, locators in locators_by_tag.items():
            try:
                keys_to_delete = [locator.key for locator in locators]
                # Batch delete in actual storage
                locators_to_interface[tag].delete_objects(keys_to_delete)
            except Exception as e:
                print(f"Failed to delete objects in ACTUAL storage for tag {tag}: {e}")

        try:
            await db.execute(
                delete(DBPhysicalObjectLocator).where(
                    tuple_(
                        DBPhysicalObjectLocator.bucket,
                        DBPhysicalObjectLocator.key,
                        DBPhysicalObjectLocator.location_tag,
                    ).in_(delete_conditions)
                )
            )
            await db.commit()
            print(f"Deleted objects in database", flush=True)
        except Exception as e:
            print(f"Failed to delete objects in database: {e}, rolling back")
            await db.rollback()
            return CleanObjectResponse(locators=[])

        # NOTE: return locator response
        locators_response = [
            LocateObjectResponse(
                id=obj.id,
                tag=obj.location_tag,
                cloud=obj.cloud,
                bucket=obj.bucket,
                region=obj.region,
                key=obj.key,
                version_id=obj.version_id,
                version=obj.logical_object_id,
            )
            for obj in objects_to_delete
        ]

        if is_skystore:
            return CleanObjectResponse(
                locators=locators_response, cost=sum(put_policy.storage_cost.values())
            )
        return CleanObjectResponse(locators=locators_response)


@router.post("/clean_out_remaining")
async def clean_out_remaining(
    request: CleanObjectRequest, db: Session = Depends(get_session)
) -> CleanObjectResponse:
    """Given the current timestamp, clean the object based on TTL."""
    put_policy = get_placement_policy(policy_ultra_dict["put_policy"], init_region_tags)
    all_objects = await db.execute(select(DBPhysicalObjectLocator))
    all_objects = all_objects.scalars().all()

    objects_to_delete_query = select(DBPhysicalObjectLocator).options(
        selectinload(DBPhysicalObjectLocator.logical_object)
    )

    objects_to_delete = await db.execute(objects_to_delete_query)
    objects_to_delete = objects_to_delete.scalars().all()

    def local_add_to_cost(cost, timedelta: int, region: str, size):
        cost[region] += (
            timedelta
            / 3600
            / 24
            * put_policy.stat_graph.nodes[region]["priceStorage"]
            * 3
            * size
            / (1024 * 1024 * 1024)
        )
        return cost

    cost_copy = put_policy.storage_cost.copy()
    for object in objects_to_delete:
        if object.location_tag != object.logical_object.base_region:
            if object.ttl != -1 and request.timestamp.replace(
                tzinfo=None
            ) >= object.storage_start_time + timedelta(seconds=object.ttl):
                cost_copy = local_add_to_cost(
                    cost_copy,
                    object.ttl,
                    object.location_tag,
                    object.logical_object.size,
                )
            else:
                cost_copy = local_add_to_cost(
                    cost_copy,
                    (
                        request.timestamp.replace(tzinfo=None)
                        - object.storage_start_time
                    ).total_seconds(),
                    object.location_tag,
                    object.logical_object.size,
                )

    locators_response = [
        LocateObjectResponse(
            id=obj.id,
            tag=obj.location_tag,
            cloud=obj.cloud,
            bucket=obj.bucket,
            region=obj.region,
            key=obj.key,
            version_id=obj.version_id,
            version=obj.logical_object_id,
        )
        for obj in objects_to_delete
    ]

    # Delete the objects if any are found
    if objects_to_delete:
        locators_by_tag = {}
        for obj in objects_to_delete:
            locators_by_tag.setdefault(obj.location_tag, []).append(obj)

        # Initialize storage interfaces
        locators_to_interface = {
            tag: create_object_store_interface(tag) for tag in locators_by_tag
        }

        for tag, locators in locators_by_tag.items():
            try:
                keys_to_delete = [locator.key for locator in locators]
                # Batch delete in actual storage
                locators_to_interface[tag].delete_objects(keys_to_delete)
            except Exception as e:
                print(f"Failed to delete objects in storage for tag {tag}: {e}")

        # Delete in database
        _ = await db.execute(
            delete(DBPhysicalObjectLocator).where(
                DBPhysicalObjectLocator.id.in_([obj.id for obj in objects_to_delete])
            )
        )
        try:
            await db.commit()
        except Exception as e:
            print(e)
            await db.rollback()
    if (
        put_policy.name() == "skystore"
        or put_policy.name() == "always_store"
        or put_policy.name() == "always_evict"
    ):
        storage_cost = sum(cost_copy.values())
        return CleanObjectResponse(
            locators=locators_response,
            storage_cost=storage_cost,
            network_cost=sum(put_policy.network_cost),
        )
    return CleanObjectResponse(locators=locators_response)
