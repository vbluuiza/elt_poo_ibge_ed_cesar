import json
from typing import Any
from pathlib import Path

class Loader:
    def load_json(self, path: str, data: Any) -> None:
        file_path = Path(path)
        
        file_path.parent.mkdir(
            parents=True,
            exist_ok=True
        )
        
        with file_path.open("w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=4)