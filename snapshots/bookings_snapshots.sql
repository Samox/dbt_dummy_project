{% snapshot bookings_snapshot %}

{{
    config(
      target_schema='public',
      unique_key='unique_key',

      strategy='check',
      check_cols=['metric_value', 'country_code'],
      updated_at='updated_at',
    )
}}

select * from  {{ref('bookings')}}

{% endsnapshot %}