create schema public;

comment on schema public is 'standard schema';

alter schema public owner to postgres;

SELECT pg_catalog.set_config('search_path', 'public', false);

create table lesson_time
(
	id bigint not null
		constraint period_pkey
			primary key,
	time_start time not null,
	time_end time not null
);

comment on table lesson_time is 'Table that represents HSE periods';

alter table lesson_time owner to postgres;

create unique index period_id_uindex
	on lesson_time (id);

create table buildings
(
	id bigint not null
		constraint buildings_pkey
			primary key,
	addr text,
	name varchar(255)
);

comment on table buildings is 'HSE Moscow buildings addresses';

alter table buildings owner to postgres;

create unique index buildings_id_uindex
	on buildings (id);

create table auditoriums
(
	id bigint not null
		constraint auditoriums_pkey
			primary key,
	building_id bigint
		constraint auditoriums_buildings_id_fk
			references buildings,
	number varchar(255),
	auditorium_type varchar(255)
);

alter table auditoriums owner to postgres;

create table students
(
	id bigserial not null
		constraint students_pkey
			primary key,
	first_name varchar(255),
	last_name varchar(255),
	patronymic_name varchar(255),
	group_name varchar(255),
	email varchar(255)
);

comment on table students is 'students there';

alter table students owner to postgres;

create table learning_courses
(
	id bigserial not null
		constraint learning_courses_pkey
			primary key,
	shortname varchar(255),
	fullname text
);

comment on table learning_courses is 'Learning courses';

alter table learning_courses owner to postgres;

create table teachers
(
	id bigint not null
		constraint teachers_pk
			primary key,
	first_name varchar(255),
	last_name varchar(255),
	patronymic_name varchar(255),
	email varchar(255)
);

alter table teachers owner to postgres;

create unique index teachers_id_uindex
	on teachers (id);

create table contingents
(
	id bigint not null
		constraint contingents_pk
			primary key,
	contingent_name varchar(255)
);

comment on table contingents is 'groups of students';

alter table contingents owner to postgres;

create table deadlines
(
	deadline_time timestamp not null,
	weight double precision,
	deadline_name varchar(255) not null,
	id bigserial not null
		constraint deadline_pk
			primary key,
	description text,
	contingent_id bigint not null
		constraint deadlines_contingents_id_fk
			references contingents
				on update cascade on delete cascade,
	course_id bigint
		constraint deadlines_learning_courses_id_fk
			references learning_courses
				on update cascade on delete cascade
);

alter table deadlines owner to postgres;

create unique index deadline_id_uindex
	on deadlines (id);

create table task_time
(
	id bigserial not null
		constraint estimated_time_pk
			primary key,
	student_id bigint
		constraint estimated_time_students_id_fk
			references students
				on update cascade on delete set null,
	deadline_id bigint not null
		constraint estimated_time_deadlines_id_fk
			references deadlines
				on update cascade on delete cascade,
	estimated_time interval not null,
	real_time interval
);

comment on table task_time is 'time, estimated by students';

alter table task_time owner to postgres;

create table lesson
(
	id bigserial not null
		constraint lesson_pk
			primary key,
	lesson_time_id bigint not null
		constraint lesson_lesson_time_id_fk
			references lesson_time
				on update cascade on delete restrict,
	auditorium_id bigint not null
		constraint lesson_auditoriums_id_fk
			references auditoriums
				on update cascade on delete restrict,
	course_id bigint not null
		constraint lesson_learning_courses_id_fk
			references learning_courses
				on update cascade on delete cascade,
	contingent_id bigint
		constraint lesson_contingents_id_fk
			references contingents
				on update cascade on delete set null,
	date date not null,
	lesson_type varchar(255),
	teacher_id bigint
		constraint lesson_teachers_id_fk
			references teachers
				on update cascade on delete set null
);

alter table lesson owner to postgres;

create table students_to_contingents
(
	student_id bigint not null
		constraint students_to_contingents_students_id_fk
			references students
				on update cascade on delete cascade,
	contingent_id bigint not null
		constraint students_to_contingents_contingents_id_fk
			references contingents,
    primary key (student_id, contingent_id)
);

comment on table students_to_contingents is 'n:n';

alter table students_to_contingents owner to postgres;




create or replace function find_users(_first_name character varying, _last_name character varying, _patronymic_name character varying) returns SETOF students
  language plpgsql
as
$$
begin
  return query
    select *
    from students
    where lower(students.first_name) like lower(concat(_first_name, '%'))
       or lower(students.last_name) like lower(concat(_last_name, '%'))
       or lower(students.patronymic_name) like lower(concat(_patronymic_name, '%'));
end;
$$;

alter function find_users(varchar, varchar, varchar) owner to postgres;


create or replace function get_contingent_id_by_user_id(_user_id bigint) returns TABLE(id bigint, name text, type character varying)
  language plpgsql
as
$$
begin
  return query
    select distinct stc.contingent_id, lc.fullname, lesson.lesson_type
    from lesson
           join contingents c on lesson.contingent_id = c.id
           join students_to_contingents stc on c.id = stc.contingent_id
           join learning_courses lc on lesson.course_id = lc.id
    where stc.student_id = _user_id;

end;
$$;

alter function get_contingent_id_by_user_id(bigint) owner to postgres;



create or replace function get_deadlines_by_id(_user_id bigint, _time_start timestamp without time zone, _time_end timestamp without time zone) returns TABLE(user_id bigint, first_name character varying, flow character varying, course_name_short character varying, course_name text, deadline_id bigint, deadline_name character varying, deadline_time timestamp without time zone, deadlines_description text, estimated_time interval, real_time interval)
  language plpgsql
as
$$
begin
  return query select students.id                 as "id",
                      students.first_name         as "first_name",
                      contingents.contingent_name as "flow",
                      courses.shortname           as "course_name_short",
                      courses.fullname            as "course_name",
                      deadlines.id                as "deadline_id",
                      deadlines.deadline_name     as "deadline_name",
                      deadlines.deadline_time     as "deadline_time",
                      deadlines.description       as "deadlines_description",
                      avg(task_time.estimated_time)    as "estimated_time",
                      avg(task_time.real_time)         as "real_time"
               from students students
                      join students_to_contingents stc on students.id = stc.student_id
                      join contingents contingents on stc.contingent_id = contingents.id
                      join deadlines deadlines on contingents.id = deadlines.contingent_id
                      left join task_time task_time on deadlines.id = task_time.deadline_id
                      join learning_courses courses on deadlines.course_id = courses.id

               where students.id = _user_id
                 and deadlines.deadline_time between _time_start and _time_end
               group by students.id, students.first_name, contingents.contingent_name, courses.shortname, courses.fullname, deadlines.id, deadlines.deadline_name, deadlines.deadline_time, deadlines.description;
end
$$;

alter function get_deadlines_by_id(bigint, timestamp, timestamp) owner to postgres;




create or replace function get_timetable_by_user_id(_user_id bigint) returns TABLE(user_id bigint, first_name character varying, lesson_time_id bigint, date date, start time without time zone, "end" time without time zone, building_addr text, lesson_type character varying, flow character varying, course_short_name character varying, course_full_name text)
  language plpgsql
as
$$
begin
  return query select students.id                 as "id",
                      students.first_name         as "first_name",
                      lessons.lesson_time_id      as "lesson_time_id",
                      lessons.date                as "date",
                      lesson_time.time_start      as "start",
                      lesson_time.time_end        as "end",
                      buildings.addr              as "building_addr",
                      lessons.lesson_type         as "type",
                      contingents.contingent_name as "flow",
                      curses.shortname            as "course_name_short",
                      curses.fullname             as "course_name"
               from students students
                      join students_to_contingents stc on students.id = stc.student_id
                      join contingents contingents on stc.contingent_id = contingents.id
                      join lesson lessons on contingents.id = lessons.contingent_id
                      join learning_courses curses on lessons.course_id = curses.id
                      join auditoriums auditoriums on lessons.auditorium_id = auditoriums.id
                      join buildings buildings on auditoriums.building_id = buildings.id
                      join lesson_time lesson_time on lessons.lesson_time_id = lesson_time.id
               where students.id = _user_id;
end
$$;

alter function get_timetable_by_user_id(bigint) owner to postgres;



create or replace function insert_deadline(_user_id bigint, _contingent_id bigint, _deadline_time timestamp without time zone, _weight double precision, _name character varying, _description text) returns void
  language plpgsql
as
$$
begin
  insert into deadlines(deadline_time, weight, deadline_name, description, contingent_id, course_id)
  values (_deadline_time, _weight, _name, _description, _contingent_id,
          (select course_id from lesson where _contingent_id=_contingent_id limit 1));
end
$$;

alter function insert_deadline(bigint, bigint, timestamp, double precision, varchar, text) owner to postgres;

ALTER DATABASE postgres SET datestyle TO "ISO, DMY";

insert into lesson_time values
(1,	'09:00:00',	'10:20:00'),
(2,	'10:30:00',	'11:50:00'),
(3,	'12:10:00',	'13:30:00'),
(4,	'13:40:00',	'15:00:00'),
(5,	'15:10:00',	'16:30:00'),
(6,	'16:40:00',	'18:00:00'),
(7,	'18:10:00',	'19:30:00'),
(8,	'19:40:00',	'21:00:00'),
(9,	'21:10:00',	'22:30:00');

