with twa_last_90d as (
	select tutor_id
	, student_id 
	, subject
	, lesson_ts
	, lag(lesson_ts) over (partition by tutor_id order by lesson_ts asc) as previous_lesson
	, duration
	, case when  datediff(minutes, previous_lesson, lesson_ts) = duration*60 then 1
		else 0
	  end as is_back_to_back_lesson
	, created_ts
	, datediff(hours, created_ts, lesson_ts) as booking_window
	, id as lesson_id
	, tutor_lesson_order
	, case when tutor_lesson_order = 1 then 1 else 0 end as first_lesson
	, case when twa_flag is true then 1 else 0 end as twa_flag
	, case when tutoring_confirmed_lesson_order = 1 and twa_flag = 0 then 1 when twa_flag = 1 and tutoring_lesson_order = 1 then 1 else 0 end as trial_lesson
	from cds.dim_lesson dl 
	where (case when twa_flag is false THEN tutoring_confirmed_lesson_order else 1 end) IS NOT null
	and lesson_ts between current_date - 180 and current_date
	)
, hour_offset_tutor as ( -- to calculate the local time for the tutors
	select dt.id as tutor_id, cast(split_part(substring(gt."offset", 5, 10), ':', 1) as integer) as hour_offset_tutor
	from cds.dim_tutor dt 
	left join etl_glossary.glos_timezone gt on gt.id = dt.timezone_id)
, hour_offset_student as ( -- to calculate the local time for the students
	select ds.id as student_id, cast(split_part(substring(gt."offset", 5, 10), ':', 1) as integer) as hour_offset_student
	from cds.dim_student ds 
	left join etl_glossary.glos_timezone gt on gt.id = ds.timezone_id)
, app_usage as ( -- does the tutor use the app?
	SELECT user_id, min(date) as first_usage_date -- min date to get the first date when the tutor logged in in the app
	FROM cds.fact_event
	WHERE event_name = 'app_auth_success'
	AND   site_version = 4
	and date  between current_date - 180 and current_date
	group by 1)
, all_lessons as (select 
	twa.*
	, rtp.performance_segment
	, rtp.activity_segment_per_tutor_subject
	, rtp.country_of_birth
	, rtp.date_first_approved
	, sum(case when lesson_ts::date > current_date-30 then twa.twa_flag else 0 end) over (partition by twa.tutor_id) as num_twa_last_30d
	, (rtp.run_dt::date - rtp.last_seen::date) as days_since_last_seen
	, case 
		when extract(month from lesson_ts) >=4 and extract(month from lesson_ts) <=11 then dateadd(hour, hour_offset_tutor + 1, lesson_ts) 
		else dateadd(hour, hour_offset_tutor, lesson_ts) 
	end as local_lesson_ts
	, local_lesson_ts::date as local_lesson_ts_date
	, extract(hour from local_lesson_ts) as local_lesson_hour
	, case 
		when extract(month from created_ts) >=4 and extract(month from created_ts) <=11 then dateadd(hour, hour_offset_tutor + 1, created_ts) 
		else dateadd(hour, hour_offset_tutor, created_ts) 
	end as local_booking_time
	, hour_offset_tutor
	, hour_offset_student
	, case 
		when hour_offset_tutor <= 0 and hour_offset_student <= 0 and hour_offset_tutor = hour_offset_student then hour_offset_tutor*-1 + hour_offset_student
		when hour_offset_tutor >= 0 and hour_offset_student >= 0 and hour_offset_tutor = hour_offset_student then hour_offset_tutor*-1 + hour_offset_student
		when hour_offset_tutor <= 0 and hour_offset_student <= 0 and hour_offset_tutor < hour_offset_student then hour_offset_tutor*-1 + hour_offset_student
		when hour_offset_tutor <= 0 and hour_offset_student <= 0 and hour_offset_tutor > hour_offset_student then hour_offset_tutor + hour_offset_student*-1
		when hour_offset_tutor >= 0 and hour_offset_student <= 0 then hour_offset_tutor + hour_offset_student*-1
		when hour_offset_tutor <= 0 and hour_offset_student >= 0 then hour_offset_tutor*-1 + hour_offset_student
		when hour_offset_tutor >= 0 and hour_offset_student >= 0 and hour_offset_tutor < hour_offset_student then hour_offset_tutor*-1 + hour_offset_student
		when hour_offset_tutor >= 0 and hour_offset_student >= 0 and hour_offset_tutor > hour_offset_student then hour_offset_tutor + hour_offset_student*-1
		end as hour_differences_tutor_student
	, local_booking_time::date as local_booking_time_date
	, extract(hour from local_booking_time) as local_booking_hour
	, count(twa.tutor_id) over (partition by twa.tutor_id, local_lesson_ts_date) as num_lessons_per_day
	, case 
		when num_lessons_per_day = 1 then 1 
		else 0
	  end as one_lesson_per_day
	, case 
		when first_usage_date <= lesson_ts::date then 1
		else 0 
	  end as app_usage -- check if the tutor had the app installed before each of the lessons
	, rtp.response_time 
from twa_last_90d twa
left join cds.rep_tutor_performance rtp on rtp.tutor_id = twa.tutor_id and rtp.subject = twa.subject and rtp.run_dt = twa.lesson_ts::date -- get all the performance metrics of the tutor at the date of each lesson
left join hour_offset_tutor ho on ho.tutor_id = twa.tutor_id
left join hour_offset_student hos on hos.student_id = twa.student_id
left join cds.dim_tutor dt on dt.id = twa.tutor_id
left join dds.tutors_tutor tt on tt.id = twa.tutor_id
left join app_usage au on au.user_id = tt.user_id
where rtp.run_dt between current_date - 180 and current_date
)
select * from all_lessons







