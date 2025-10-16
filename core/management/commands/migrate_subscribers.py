"""
Fix: avoid "Models aren't loaded yet" by deferring model resolution until
Django's app registry is ready (i.e., inside `handle()`), instead of at import time.

Also keeps the previous fix (no hard-coded app label). You can optionally set
`GDPR_MODELS_APP_LABEL` in settings to disambiguate models if names collide.
"""

# —— IMPORTS ——
from django.core.management.base import BaseCommand
from django.db import transaction
from django.utils import timezone
from django.apps import apps
import csv
from collections import defaultdict

# —— SETTINGS ——
APP_LABEL_SETTING = "GDPR_MODELS_APP_LABEL"


def _get_model_by_name(model_name: str):
    """Return a Django model class by its class name across all apps.

    Must be called only after the app registry is ready
    (inside management command's handle() or after django.setup()).

    Priority:
    1) If `settings.GDPR_MODELS_APP_LABEL` is defined, try that app first.
    2) Otherwise, scan all registered apps for a unique class name match.
    """
    from django.conf import settings

    if not apps.ready:
        # Defensive: make failure mode explicit during development/tests
        raise RuntimeError("Apps registry is not ready; call after django.setup() / inside handle().")

    app_label = getattr(settings, APP_LABEL_SETTING, None)
    if app_label:
        model = apps.get_model(app_label, model_name)
        if model is None:
            raise LookupError(
                f"Model '{model_name}' not found in app '{app_label}'. "
                f"Check {APP_LABEL_SETTING} or your INSTALLED_APPS."
            )
        return model

    matches = [m for m in apps.get_models() if m.__name__ == model_name]
    if not matches:
        raise LookupError(
            f"Model '{model_name}' not found in any installed app. "
            f"Ensure the app is in INSTALLED_APPS and migrations are applied."
        )
    if len(matches) > 1:
        labels = ", ".join(sorted({m._meta.app_label for m in matches}))
        raise LookupError(
            f"Ambiguous model name '{model_name}' found in multiple apps: {labels}. "
            f"Set settings.{APP_LABEL_SETTING} to the correct app label."
        )
    return matches[0]


class Command(BaseCommand):
    """Migracja Subscriber & SubscriberSMS -> User zgodnie z wymogami PDF.
    - email-first dla Subscriber
    - phone-first dla SubscriberSMS
    - CSV z konfliktami i nieunikalnymi telefonami klientów
    - przeniesienie gdpr_consent przy tworzeniu nowych User
    """

    help = "Migrates Subscriber & SubscriberSMS to User per the spec."

    # ---------- internal: resolve models lazily ----------
    def _resolve_models(self):
        Subscriber = _get_model_by_name("Subscriber")
        SubscriberSMS = _get_model_by_name("SubscriberSMS")
        Client = _get_model_by_name("Client")
        User = _get_model_by_name("User")
        return Subscriber, SubscriberSMS, Client, User

    def handle(self, *args, **options):
        # Resolve models *after* registry is ready
        Subscriber, SubscriberSMS, Client, User = self._resolve_models()

        # — PRELOAD — minimalizujemy liczbę zapytań (kryterium oceny)
        self.stdout.write(self.style.NOTICE("Preparing lookups…"))

        # Wczytujemy wszystkich User do słowników O(1) — najpierw po email
        user_by_email = {u.email: u for u in User.objects.only("id", "email", "phone", "create_date")}

        # Następnie po phone -> lista (możliwa nieunikalność legacy)
        user_by_phone = defaultdict(list)
        for u in User.objects.only("id", "email", "phone", "create_date"):
            if u.phone:
                user_by_phone[u.phone].append(u)

        # Wczytujemy Client (łączymy później po email/phone)
        clients = list(Client.objects.only("id", "email", "phone", "create_date"))

        # Mapowanie Client po email
        client_by_email = {c.email: c for c in clients}

        # Zliczamy telefony klientów -> nieunikalne numery
        phone_counts = defaultdict(int)
        for c in clients:
            if c.phone:
                phone_counts[c.phone] += 1
        non_unique_client_phones = {p for p, cnt in phone_counts.items() if cnt > 1}

        # CSV z nieunikalnymi telefonami
        self._write_non_unique_phones_csv(clients, non_unique_client_phones)

        # — GAŁĄŹ 1: Subscriber (email-first) —
        self.stdout.write(self.style.NOTICE("Migrating from Subscriber…"))
        self._migrate_from_subscribers(
            Subscriber,
            User,
            user_by_email,
            user_by_phone,
            client_by_email,
            non_unique_client_phones,
        )

        # Odśwież cache po ewentualnych insertach
        user_by_email = {u.email: u for u in User.objects.only("id", "email", "phone", "create_date")}
        user_by_phone = defaultdict(list)
        for u in User.objects.only("id", "email", "phone", "create_date"):
            if u.phone:
                user_by_phone[u.phone].append(u)

        # — GAŁĄŹ 2: SubscriberSMS (phone-first; odwrotna logika + osobny CSV) —
        self.stdout.write(self.style.NOTICE("Migrating from SubscriberSMS…"))
        self._migrate_from_subscribersms(
            SubscriberSMS,
            User,
            clients,
            user_by_email,
            user_by_phone,
            non_unique_client_phones,
        )

        self.stdout.write(self.style.SUCCESS("Done."))

    # —— CSV HELPERS ——
    def _write_non_unique_phones_csv(self, clients, non_unique_set):
        rows = [
            {"client_id": c.id, "phone": c.phone, "email": c.email}
            for c in clients
            if c.phone in non_unique_set
        ]
        with open("non_unique_client_phones.csv", "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=["client_id", "phone", "email"])
            w.writeheader()
            w.writerows(rows)

    def _append_conflict(self, path, row):
        header = list(row.keys())
        try:
            with open(path, "x", newline="", encoding="utf-8") as f:  # create-only
                w = csv.DictWriter(f, fieldnames=header)
                w.writeheader()
                w.writerow(row)
        except FileExistsError:
            with open(path, "a", newline="", encoding="utf-8") as f:  # append
                w = csv.DictWriter(f, fieldnames=header)
                w.writerow(row)

    # —— BUSINESS RULES: Subscriber (email-first) ——
    def _migrate_from_subscribers(self, Subscriber, User, user_by_email, user_by_phone, client_by_email, non_unique_client_phones):
        qs = (
            Subscriber.objects
            .all()
            .only("id", "email", "gdpr_consent", "create_date")
            .order_by("id")
        )
        buffer = []
        created = skipped = conflicts = 0

        for sub in qs.iterator(chunk_size=1000):
            # 1) istnieje User.email == Subscriber.email ? -> pomiń
            if user_by_email.get(sub.email):
                skipped += 1
                continue

            # 2) Spróbuj dopasować Client po email
            client = client_by_email.get(sub.email)

            if client:
                # 2.a) telefon klienta nieunikalny -> nie tworzymy, log do CSV
                if client.phone and client.phone in non_unique_client_phones:
                    self._append_conflict(
                        "non_unique_client_phones.csv",
                        {"client_id": client.id, "phone": client.phone, "email": client.email},
                    )
                    skipped += 1
                    continue

                # 2.b) KONFLIKT: istnieje User z phone == Client.phone i email != Client.email
                conflict_users = [uu for uu in (user_by_phone.get(client.phone) or []) if uu.email != client.email]
                if conflict_users:
                    self._append_conflict(
                        "subscriber_conflicts.csv",
                        {
                            "subscriber_id": sub.id,
                            "subscriber_email": sub.email,
                            "client_phone": client.phone,
                            "client_email": client.email,
                        },
                    )
                    conflicts += 1
                    continue

                # 2.c) BRAK konfliktu -> tworzymy User na podstawie Client (przenosimy gdpr_consent z Subscriber)
                buffer.append(
                    User(
                        email=client.email,
                        phone=client.phone,
                        gdpr_consent=sub.gdpr_consent,
                        create_date=timezone.now(),
                    )
                )
            else:
                # 3) Brak Client o tym samym email -> tworzymy User z pustym phone
                buffer.append(
                    User(
                        email=sub.email,
                        phone=None,
                        gdpr_consent=sub.gdpr_consent,
                        create_date=timezone.now(),
                    )
                )

            # Flush w paczkach
            if len(buffer) >= 1000:
                created += self._flush_users(buffer, user_by_email, user_by_phone)

        # Flush końcowy
        created += self._flush_users(buffer, user_by_email, user_by_phone)
        self.stdout.write(self.style.SUCCESS(f"Subscriber: created={created}, skipped={skipped}, conflicts={conflicts}"))

    # —— BUSINESS RULES: SubscriberSMS (phone-first, odwrócone pola) ——
    def _migrate_from_subscribersms(self, SubscriberSMS, User, clients, user_by_email, user_by_phone, non_unique_client_phones):
        # pomocnicze mapowanie phone -> [Client]
        client_by_phone = {}
        for c in clients:
            if c.phone:
                client_by_phone.setdefault(c.phone, []).append(c)

        qs = (
            SubscriberSMS.objects
            .all()
            .only("id", "phone", "gdpr_consent", "create_date")
            .order_by("id")
        )
        buffer = []
        created = skipped = conflicts = 0

        for sms in qs.iterator(chunk_size=1000):
            # 1) istnieje User.phone == SubscriberSMS.phone ? -> pomiń
            if (user_by_phone.get(sms.phone) or []):
                skipped += 1
                continue

            # 2) dopasuj Client(ów) po phone
            clients_for_phone = client_by_phone.get(sms.phone) or []

            if not clients_for_phone:
                # 2.a) Brak klienta -> tworzymy User z pustym email
                buffer.append(
                    User(
                        email=None,
                        phone=sms.phone,
                        gdpr_consent=sms.gdpr_consent,
                        create_date=timezone.now(),
                    )
                )
            else:
                # 2.b) Telefon nieunikalny (wielu klientów) -> nie tworzymy, log do CSV
                if sms.phone in non_unique_client_phones:
                    for c in clients_for_phone:
                        self._append_conflict(
                            "non_unique_client_phones.csv",
                            {"client_id": c.id, "phone": c.phone, "email": c.email},
                        )
                    skipped += 1
                    continue

                # 2.c) Unikalny klient -> sprawdź KONFLIKT odwrotny: User.email == Client.email i inny phone
                client = clients_for_phone[0]
                u_conflict = user_by_email.get(client.email)
                if u_conflict and u_conflict.phone != client.phone:
                    self._append_conflict(
                        "subscribersms_conflicts.csv",
                        {
                            "subscribersms_id": sms.id,
                            "subscribersms_phone": sms.phone,
                            "client_phone": client.phone,
                            "client_email": client.email,
                        },
                    )
                    conflicts += 1
                    continue

                # 2.d) Brak konfliktu -> tworzymy User na podstawie Client, przenosząc gdpr_consent z SMS
                buffer.append(
                    User(
                        email=client.email,
                        phone=client.phone,
                        gdpr_consent=sms.gdpr_consent,
                        create_date=timezone.now(),
                    )
                )

            # Flush w paczkach
            if len(buffer) >= 1000:
                created += self._flush_users(buffer, user_by_email, user_by_phone)

        # Flush końcowy
        created += self._flush_users(buffer, user_by_email, user_by_phone)
        self.stdout.write(self.style.SUCCESS(f"SubscriberSMS: created={created}, skipped={skipped}, conflicts={conflicts}"))

    # —— FLUSH ——
    def _flush_users(self, buffer, user_by_email, user_by_phone):
        if not buffer:
            return 0
        with transaction.atomic():
            to_insert = []
            for u in buffer:
                if u.email and u.email in user_by_email:
                    continue
                if u.phone and any(user_by_phone.get(u.phone, [])):
                    continue
                to_insert.append(u)

            created_objs = []
            if to_insert:
                from django.db import IntegrityError
                try:
                    created_objs = type(to_insert[0]).objects.bulk_create(to_insert, ignore_conflicts=True)
                except IntegrityError:
                    # Fallback for older DBs; insert one-by-one (still guarded by idempotency checks)
                    for obj in to_insert:
                        try:
                            obj.save(force_insert=True)
                            created_objs.append(obj)
                        except IntegrityError:
                            pass

            for u in created_objs:
                if u.email:
                    user_by_email[u.email] = u
                if u.phone:
                    user_by_phone[u.phone].append(u)
        buffer.clear()
        return len(created_objs)
