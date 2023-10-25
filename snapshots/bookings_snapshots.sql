{% snapshot bookings_snapshot %}

{{
    config(
      target_schema='public',
      unique_key='unique_key',

      strategy='timestamp',
      updated_at='updated_at',
    )
}}

select * from  {{ref('bookings')}}

{% endsnapshot %}