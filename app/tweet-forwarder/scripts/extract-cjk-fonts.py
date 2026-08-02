import os
from pathlib import Path

from fontTools.ttLib import TTCollection

OUTPUT_DIR = Path(os.environ.get("OUTPUT_DIR", "/app/assets/fonts"))
FONTS = [
    ("/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc", 0, "NotoSansCJKjp-Regular.otf"),
    ("/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc", 2, "NotoSansCJKsc-Regular.otf"),
    ("/usr/share/fonts/opentype/noto/NotoSansCJK-Bold.ttc", 0, "NotoSansCJKjp-Bold.otf"),
    ("/usr/share/fonts/opentype/noto/NotoSansCJK-Bold.ttc", 2, "NotoSansCJKsc-Bold.otf"),
]

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
for source, index, filename in FONTS:
    output = OUTPUT_DIR / filename
    font = TTCollection(source).fonts[index]
    font.save(output)
    if output.read_bytes()[:4] != b"OTTO":
        raise RuntimeError(f"Invalid extracted font: {output}")
