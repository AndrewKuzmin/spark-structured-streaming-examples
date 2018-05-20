package com.phylosoft.spark.learning.sql.streaming.data

import org.apache.spark.sql.types._

/**
  * Created by Andrew on 5/20/2018.
  */
object JsonSchemas {

  //  https://developers.nest.com/documentation/api-reference
  val NEST_SCHEMA: StructType = new StructType()
    .add("devices",
      new StructType()
        .add("thermostats", MapType(StringType,
          new StructType()
            .add("device_id", StringType)
            .add("locale", StringType)
            .add("software_version", StringType)
            .add("structure_id", StringType)
            .add("where_name", StringType)
            .add("last_connection", StringType)
            .add("is_online", BooleanType)
            .add("can_cool", BooleanType)
            .add("can_heat", BooleanType)
            .add("is_using_emergency_heat", BooleanType)
            .add("has_fan", BooleanType)
            .add("fan_timer_active", BooleanType)
            .add("fan_timer_timeout", StringType)
            .add("temperature_scale", StringType)
            .add("target_temperature_f", DoubleType)
            .add("target_temperature_high_f", DoubleType)
            .add("target_temperature_low_f", DoubleType)
            .add("eco_temperature_high_f", DoubleType)
            .add("eco_temperature_low_f", DoubleType)
            .add("away_temperature_high_f", DoubleType)
            .add("away_temperature_low_f", DoubleType)
            .add("hvac_mode", StringType)
            .add("humidity", LongType)
            .add("hvac_state", StringType)
            .add("is_locked", StringType)
            .add("locked_temp_min_f", DoubleType)
            .add("locked_temp_max_f", DoubleType)))
        .add("smoke_co_alarms", MapType(StringType,
          new StructType()
            .add("device_id", StringType)
            .add("locale", StringType)
            .add("software_version", StringType)
            .add("structure_id", StringType)
            .add("where_name", StringType)
            .add("last_connection", StringType)
            .add("is_online", BooleanType)
            .add("battery_health", StringType)
            .add("co_alarm_state", StringType)
            .add("smoke_alarm_state", StringType)
            .add("is_manual_test_active", BooleanType)
            .add("last_manual_test_time", StringType)
            .add("ui_color_state", StringType)))
        .add("cameras", MapType(StringType,
          new StructType()
            .add("device_id", StringType)
            .add("software_version", StringType)
            .add("structure_id", StringType)
            .add("where_name", StringType)
            .add("is_online", BooleanType)
            .add("is_streaming", BooleanType)
            .add("is_audio_input_enabled", BooleanType)
            .add("last_is_online_change", StringType)
            .add("is_video_history_enabled", BooleanType)
            .add("web_url", StringType)
            .add("app_url", StringType)
            .add("is_public_share_enabled", BooleanType)
            .add("activity_zones",
              new StructType()
                .add("name", StringType)
                .add("id", LongType))
            .add("last_event", StringType))
        )
    )

}
