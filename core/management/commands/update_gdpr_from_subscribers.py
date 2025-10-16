"""
Task 2 command. Defers model resolution until registry is ready to avoid
"Models aren't loaded yet".
"""

from django.core.management.base import BaseCommand
from django.db import transaction
from django.apps import apps

APP_LABEL_SETTING = "GDPR_MODELS_APP_LABEL"


def _get_model_by_name(model_name: str):
    from django.conf import settings
    if not apps.ready:
        raise RuntimeError("Apps registry is not ready; call after django.setup() / inside handle().")

    app_label = getattr(settings, APP_LABEL_SETTING, None)
    if app_label:
        model = apps.get_model(app_label, model_name)
        if model is None:
            raise LookupError(
                f"Model '{model_name}' not found in app '{app_label}'. Set {APP_LABEL_SETTING}."
            )
        return model

    matches = [m for m in apps.get_models() if m.__name__ == model_name]
    if not matches:
        raise LookupError(f"Model '{model_name}' not found across INSTALLED_APPS.")
    if len(matches) > 1:
        labels = ", ".join(sorted({m._meta.app_label for m in matches}))
        raise LookupError(
            f"Ambiguous model name '{model_name}' in apps: {labels}. Set {APP_LABEL_SETTING}."
        )
    return matches[0]


class Command(BaseCommand):
    """Uzupełnia/aktualizuje User.gdpr_consent zgodnie z Zadaniem 2:
    - Dla pominiętych odpowiedników (User<->Subscriber lub User<->SubscriberSMS):
      jeśli create_date subskrybenta jest nowszy niż User.create_date -> aktualizuj zgodę.
    - Jeśli User powstał z "połączenia" (istnieje Client z tym samym email i phone; oba odpowiedniki
      w Subscriber i SubscriberSMS): wygrywa najnowszy obiekt (max po create_date).
    """

    help = "Updates User.gdpr_consent from matching Subscribers/SMS when subscriber create_date is newer."

    def _resolve_models(self):
        Subscriber = _get_model_by_name("Subscriber")
        SubscriberSMS = _get_model_by_name("SubscriberSMS")
        Client = _get_model_by_name("Client")
        User = _get_model_by_name("User")
        return Subscriber, SubscriberSMS, Client, User

    def handle(self, *args, **options):
        Subscriber, SubscriberSMS, Client, User = self._resolve_models()

        # — PRELOAD —
        user_by_email = {
            u.email: u
            for u in User.objects.only("id", "email", "phone", "gdpr_consent", "create_date")
            if u.email
        }
        users_with_phone = {
            u.phone: u
            for u in User.objects.only("id", "email", "phone", "gdpr_consent", "create_date")
            if u.phone
        }

        subs = list(Subscriber.objects.only("id", "email", "gdpr_consent", "create_date"))
        sms_list = list(SubscriberSMS.objects.only("id", "phone", "gdpr_consent", "create_date"))

        clients = list(Client.objects.only("id", "email", "phone"))
        client_by_email = {c.email: c for c in clients}
        client_by_phone = {}
        for c in clients:
            if c.phone:
                client_by_phone.setdefault(c.phone, []).append(c)

        updates = []

        # 1) Subscriber.email == User.email -> jeśli Subscriber.create_date > User.create_date -> przepisz gdpr
        for s in subs:
            u = user_by_email.get(s.email)
            if not u:
                continue
            if s.create_date and u.create_date and s.create_date > u.create_date:
                if u.gdpr_consent != s.gdpr_consent:
                    u.gdpr_consent = s.gdpr_consent
                    updates.append(u)

        # 2) SubscriberSMS.phone == User.phone -> analogicznie
        for sm in sms_list:
            u = users_with_phone.get(sm.phone)
            if not u:
                continue
            if sm.create_date and u.create_date and sm.create_date > u.create_date:
                if u.gdpr_consent != sm.gdpr_consent:
                    u.gdpr_consent = sm.gdpr_consent
                    updates.append(u)

        # 3) Przypadek "połączenia"
        seen = set()
        unique_updates = []
        for u in updates:
            if u.id not in seen:
                seen.add(u.id)
                unique_updates.append(u)

        candidates = list(user_by_email.values())
        for u in candidates:
            if not (u.email and u.phone):
                continue
            c = client_by_email.get(u.email)
            if not (c and c.phone == u.phone):
                continue
            s = next((x for x in subs if x.email == u.email), None)
            sm = next((x for x in sms_list if x.phone == u.phone), None)
            if not (s and sm):
                continue
            latest = max(
                [
                    (s.gdpr_consent, s.create_date, "sub"),
                    (sm.gdpr_consent, sm.create_date, "sms"),
                ],
                key=lambda t: t[1] or u.create_date,
            )
            consent = latest[0]
            if consent != u.gdpr_consent:
                u.gdpr_consent = consent
                unique_updates.append(u)

        final_updates = {}
        for u in unique_updates:
            final_updates[u.id] = u

        with transaction.atomic():
            if final_updates:
                User.objects.bulk_update(final_updates.values(), ["gdpr_consent"])

        self.stdout.write(self.style.SUCCESS(f"Updated {len(final_updates)} users."))


