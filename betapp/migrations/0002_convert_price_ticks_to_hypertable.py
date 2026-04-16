from django.db import migrations

class Migration(migrations.Migration):

    dependencies = [
        ('betapp', '0001_initial'),
    ]

    operations = [
        migrations.RunSQL(
            sql="""
                ALTER TABLE price_ticks DROP CONSTRAINT IF EXISTS price_ticks_pkey;

                SELECT create_hypertable(
                    'price_ticks',
                    'tick_time',
                    chunk_time_interval => INTERVAL '1 week',
                    if_not_exists => TRUE
                );
            """,
            reverse_sql="""
                SELECT 1;
            """
        ),
    ]