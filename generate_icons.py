# generate_icons.py
# Run this once locally to generate placeholder tray icons
# python generate_icons.py

from PIL import Image, ImageDraw
import os
import math

os.makedirs("assets", exist_ok=True)

def make_circle(color, size=64):
    img = Image.new("RGBA", (size, size), (0, 0, 0, 0))
    draw = ImageDraw.Draw(img)
    draw.ellipse([4, 4, size-4, size-4], fill=color)
    return img

# Connected — green
make_circle((40, 200, 40)).save("assets/icon_connected.png")
print("Created assets/icon_connected.png")

# Disconnected — red
make_circle((200, 40, 40)).save("assets/icon_disconnected.png")
print("Created assets/icon_disconnected.png")

# Syncing frames — animated blue
for i in range(8):
    angle = i / 8
    brightness = int(100 + 155 * abs(math.sin(angle * math.pi)))
    make_circle((0, brightness, 255)).save(f"assets/icon_syncing_{i}.png")
    print(f"Created assets/icon_syncing_{i}.png")

print("\nDone! All icons generated in assets/")
