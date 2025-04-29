from fastapi import APIRouter, HTTPException
from typing import List, Dict, Any
from app.services.storage_service import list_files, generate_signed_url

router = APIRouter()


def build_recursive_tree(file_keys: List[str]) -> List[Dict[str, Any]]:
    tree: Dict[str, Any] = {}

    for key in file_keys:
        if key.endswith('/'):
            continue

        # Skip the first part (hotelId)
        parts = key.strip('/').split('/')[1:]

        # Only include 'contracts' and 'reports'
        if not parts or parts[0] not in ['contracts', 'reports']:
            continue

        current = tree
        for i, part in enumerate(parts):
            if i == len(parts) - 1:
                # File node (✅ NO signed URL here – defer to GET route)
                current.setdefault(part, {
                    "name": part,
                    "path": '/'.join(parts)
                })
            else:
                current = current.setdefault(part, {
                    "name": part,
                    "children": {}
                })["children"]

    def convert_to_list(node: Dict[str, Any]) -> List[Dict[str, Any]]:
        result = []
        for value in node.values():
            if "children" in value:
                value["children"] = convert_to_list(value["children"])
            result.append(value)
        return result

    return convert_to_list(tree)


@router.get("/files/tree/{hotel_id}")
async def get_recursive_file_tree(hotel_id: str) -> List[Dict[str, Any]]:
    try:
        prefix = f"{hotel_id}/"
        objects = list_files(prefix)

        keys = [obj["Key"] for obj in objects if not obj["Key"].endswith("/")]

        return build_recursive_tree(keys)

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error building file tree: {str(e)}")


# ✅ NEW ROUTE: get signed URL for a specific service report file
@router.get("/reports/{hotel_id}/{path:path}")
async def get_service_report_file(hotel_id: str, path: str):
    key = f"{hotel_id}/{path}"
    try:
        signed_url = generate_signed_url(key)
        return {"url": signed_url}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to generate signed URL: {str(e)}")
