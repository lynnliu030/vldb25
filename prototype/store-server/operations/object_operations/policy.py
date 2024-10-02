from operations.utils.helper import policy_ultra_dict
from operations.schemas.object_schemas import SetPolicyRequest
from operations.utils.db import get_session
from sqlalchemy.orm import Session
from fastapi import Depends, APIRouter


# Initialize API router
router = APIRouter()


@router.post("/update_policy")
async def update_policy(
    request: SetPolicyRequest, db: Session = Depends(get_session)
) -> None:
    put_policy_type = request.put_policy
    get_policy_type = request.get_policy

    print(f"Policy update - put: {put_policy_type}, get: {get_policy_type}")

    old_put_policy_type = policy_ultra_dict["put_policy"]
    old_get_policy_type = policy_ultra_dict["get_policy"]

    if put_policy_type is None and get_policy_type is None:
        raise ValueError("Invalid policy type")

    if put_policy_type is not None and put_policy_type != old_put_policy_type:
        policy_ultra_dict["put_policy"] = put_policy_type

    if get_policy_type is not None and get_policy_type != old_get_policy_type:
        policy_ultra_dict["get_policy"] = get_policy_type
