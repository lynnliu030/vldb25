import asyncio
from datetime import datetime, timedelta
from dotenv import load_dotenv
from sqlalchemy import select, update

from fastapi import FastAPI
from fastapi.routing import APIRoute
from operations.utils.conf import Base
from sqlalchemy import delete

from operations.schemas.object_schemas import (
    DBLogicalObject,
    DBPhysicalObjectLocator,
    Status,
    HealthcheckResponse,
)
from operations.schemas.bucket_schemas import DBLogicalBucket, DBPhysicalBucketLocator
from operations.utils.db import engine

# Import routers
from operations.bucket_operations.create import router as bucket_create_router
from operations.bucket_operations.delete import router as bucket_delete_router
from operations.bucket_operations.locate import router as bucket_locate_router
from operations.bucket_operations.metadata import router as bucket_metadata_router

from operations.object_operations.policy import router as object_update_policy_router
from operations.object_operations.delete import router as object_delete_router
from operations.object_operations.warmup import router as object_warmup_router
from operations.object_operations.metadata import router as object_metadata_router
from operations.object_operations.multipart import router as object_multipart_router
from operations.object_operations.put import router as object_put_router
from operations.object_operations.get import router as object_get_router
from operations.object_operations.clean import router as object_clean_router

app = FastAPI()

load_dotenv()
app.include_router(bucket_create_router)
app.include_router(bucket_delete_router)
app.include_router(bucket_locate_router)
app.include_router(bucket_metadata_router)

app.include_router(object_update_policy_router)
app.include_router(object_delete_router)
app.include_router(object_warmup_router)
app.include_router(object_metadata_router)
app.include_router(object_multipart_router)
app.include_router(object_put_router)
app.include_router(object_get_router)
app.include_router(object_clean_router)

stop_task_flag = asyncio.Event()
background_tasks = set()


async def rm_lock_on_timeout(minutes: int = 100, test: bool = False):
    if not test:
        await asyncio.sleep(minutes)
    while not stop_task_flag.is_set() or test:
        async with engine.begin() as db:
            cutoff_time = datetime.utcnow() - timedelta(minutes)
            stmt_timeout_physical_objects = (
                update(DBPhysicalObjectLocator)
                .where(DBPhysicalObjectLocator.lock_acquired_ts <= cutoff_time)
                .values(status=Status.ready, lock_acquired_ts=None)
            )
            await db.execute(stmt_timeout_physical_objects)

            stmt_timeout_physical_buckets = (
                update(DBPhysicalBucketLocator)
                .where(DBPhysicalBucketLocator.lock_acquired_ts <= cutoff_time)
                .values(status=Status.ready, lock_acquired_ts=None)
            )
            await db.execute(stmt_timeout_physical_buckets)

            stmt_find_pending_logical_objs = select(DBLogicalObject).where(
                DBLogicalObject.status == Status.pending
            )
            pendingLogicalObjs = (
                await db.execute(stmt_find_pending_logical_objs)
            ).fetchall()

            if pendingLogicalObjs is not None:
                for logical_obj in pendingLogicalObjs:
                    stmt3 = (
                        select(DBPhysicalObjectLocator)
                        .join(DBLogicalObject)
                        .where(
                            logical_obj.id == DBPhysicalObjectLocator.logical_object_id
                        )
                    )
                    objects = (await db.execute(stmt3)).fetchall()
                    if all([Status.ready == obj.status for obj in objects]):
                        edit_logical_obj_stmt = (
                            update(DBLogicalObject)
                            .where(objects[0].logical_object_id == logical_obj.id)
                            .values(status=Status.ready)
                        )
                        await db.execute(edit_logical_obj_stmt)

            stmt_find_pending_logical_buckets = select(DBLogicalBucket).where(
                DBLogicalBucket.status == Status.pending
            )
            pendingLogicalBuckets = (
                await db.execute(stmt_find_pending_logical_buckets)
            ).fetchall()

            if pendingLogicalBuckets is not None:
                for logical_bucket in pendingLogicalBuckets:
                    stmt3 = (
                        select(DBPhysicalBucketLocator)
                        .join(DBLogicalBucket)
                        .where(
                            logical_bucket.id
                            == DBPhysicalBucketLocator.logical_bucket_id
                        )
                    )
                    buckets = (await db.execute(stmt3)).fetchall()

                    if all([Status.ready == bucket.status for bucket in buckets]):
                        edit_logical_bucket_stmt = (
                            update(DBLogicalBucket)
                            .where(buckets[0].logical_bucket_id == logical_bucket.id)
                            .values(status=Status.ready)
                        )
                        await db.execute(edit_logical_bucket_stmt)

            await db.commit()

        if test:
            break

        await asyncio.sleep(minutes * 60)


@app.on_event("shutdown")
async def shutdown_event():
    stop_task_flag.set()
    background_tasks.discard


@app.on_event("startup")
async def startup():
    startup_time = 0
    while True:
        try:
            async with engine.begin() as conn:
                await conn.run_sync(Base.metadata.create_all)
                break
        except Exception:
            print("Database still creating, waiting for 5 seconds...")
            await asyncio.sleep(5)
            startup_time += 5
            if startup_time > 10:
                print("Database creation TIMEOUT! Exiting...")
                break

    # NOTE: background removing lock and background cleanup
    task = asyncio.create_task(rm_lock_on_timeout())
    background_tasks.add(task)


@app.get("/healthz")
async def healthz() -> HealthcheckResponse:
    return HealthcheckResponse(status="OK")


def use_route_names_as_operation_ids(app: FastAPI) -> None:
    """
    Simplify operation IDs so that generated API clients have simpler function
    names.

    Should be called only after all routes have been added.
    """
    for route in app.routes:
        if isinstance(route, APIRoute):
            route.operation_id = route.name


use_route_names_as_operation_ids(app)
