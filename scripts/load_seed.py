# scripts/load_seed.py  — loader seeda (Windows/macOS/Linux)

import os
import sys
import csv

# 1) Dołóż katalog projektu na sys.path (…\migration), bo ten plik leży w …\migration\scripts
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)


os.environ.setdefault("DJANGO_SETTINGS_MODULE", "migration.settings")


import django  # noqa: E402
django.setup()

# 4) Teraz można używać Django
from django.apps import apps  # noqa: E402
from django.utils.dateparse import parse_datetime  # noqa: E402


def get_model(name: str):
    matches = [m for m in apps.get_models() if m.__name__ == name]
    if not matches:
        raise LookupError(f"Model {name} not found. Is your app in INSTALLED_APPS and migrated?")
    if len(matches) > 1:
        labels = ", ".join(sorted({m._meta.app_label for m in matches}))
        raise LookupError(f"Ambiguous model {name}: {labels}")
    return matches[0]


Client        = get_model("Client")
Subscriber    = get_model("Subscriber")
SubscriberSMS = get_model("SubscriberSMS")
User          = get_model("User")


def upsert(Model, rid, defaults):
    Model.objects.update_or_create(id=rid, defaults=defaults)


def load_csv(Model, path, mapper):
    # Prosty guard na brak pliku:
    if not os.path.exists(path):
        raise FileNotFoundError(f"CSV not found: {path}")
    with open(path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            rid, defaults = mapper(row)
            upsert(Model, rid, defaults)


# --- ŚCIEŻKI dopasowane do Twojego repo (uruchamiaj z katalogu, gdzie jest manage.py) ---
load_csv(Client,        r"core\test_data\clients_csv.csv",       lambda r: (int(r["id"]), {
    "email": r.get("email") or None, "phone": r.get("phone") or None,
    "create_date": parse_datetime(r["create_date"]) if r.get("create_date") else None
}))
load_csv(Subscriber,    r"core\test_data\subscribers_csv.csv",   lambda r: (int(r["id"]), {
    "email": r["email"], "gdpr_consent": str(r["gdpr_consent"]).lower() == "true",
    "create_date": parse_datetime(r["create_date"]) if r.get("create_date") else None
}))
load_csv(SubscriberSMS, r"core\test_data\subscribersms_csv.csv", lambda r: (int(r["id"]), {
    "phone": r["phone"], "gdpr_consent": str(r["gdpr_consent"]).lower() == "true",
    "create_date": parse_datetime(r["create_date"]) if r.get("create_date") else None
}))
load_csv(User,          r"core\test_data\users_csv.csv",         lambda r: (int(r["id"]), {
    "email": r.get("email") or None, "phone": r.get("phone") or None,
    "gdpr_consent": str(r["gdpr_consent"]).lower() == "true",
    "create_date": parse_datetime(r["create_date"]) if r.get("create_date") else None
}))

print("Seed OK")
