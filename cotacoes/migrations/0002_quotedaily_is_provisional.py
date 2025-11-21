from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("cotacoes", "0001_initial"),
    ]

    operations = [
        migrations.AddField(
            model_name="quotedaily",
            name="is_provisional",
            field=models.BooleanField(default=False),
        ),
    ]
