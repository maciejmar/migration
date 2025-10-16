from django.db import models



class Subscriber(models.Model):
    email = models.EmailField(null=False, blank=False)
    gdpr_consent = models.BooleanField(default=False)
    create_date = models.DateTimeField(auto_now_add=True)

    class Meta:
        indexes = [
            models.Index(fields=["email"]),
            models.Index(fields=["create_date"]),
        ]


class SubscriberSMS(models.Model):
    phone = models.CharField(max_length=32, null=False, blank=False)
    gdpr_consent = models.BooleanField(default=False)
    create_date = models.DateTimeField(auto_now_add=True)

    class Meta:
        indexes = [
            models.Index(fields=["phone"]),
            models.Index(fields=["create_date"]),
        ]


class Client(models.Model):
    email = models.EmailField(null=True, blank=True)
    phone = models.CharField(max_length=32, null=True, blank=True)
    create_date = models.DateTimeField(auto_now_add=True)

    class Meta:
        indexes = [
            models.Index(fields=["email"]),
            models.Index(fields=["phone"]),
            models.Index(fields=["create_date"]),
        ]


class User(models.Model):
    email = models.EmailField(null=True, blank=True)
    phone = models.CharField(max_length=32, null=True, blank=True)
    gdpr_consent = models.BooleanField(default=False)
    create_date = models.DateTimeField(auto_now_add=True)

    class Meta:
        indexes = [
            models.Index(fields=["email"]),
            models.Index(fields=["phone"]),
            models.Index(fields=["create_date"]),
        ]

