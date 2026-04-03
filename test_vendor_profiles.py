from vendor_profile_store import load_vendor_profiles
import json

profiles = load_vendor_profiles()
print(json.dumps(profiles, indent=2))