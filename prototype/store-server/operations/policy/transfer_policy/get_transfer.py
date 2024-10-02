from operations.policy.transfer_policy.policy_cheapest import CheapestTransfer
from operations.policy.transfer_policy.policy_closest import ClosestTransfer
from operations.policy.transfer_policy.policy_direct import DirectTransfer
from operations.policy.transfer_policy.base import TransferPolicy
from operations.policy.transfer_policy.policy_manual import Manual


def get_transfer_policy(name: str) -> TransferPolicy:
    if name == "cheapest":
        return CheapestTransfer()
    elif name == "closest":
        return ClosestTransfer()
    elif name == "direct":
        return DirectTransfer()
    elif name == "manual":
        return Manual()
    else:
        raise Exception("Unknown transfer policy name")
