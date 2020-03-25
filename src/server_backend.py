import logging
import os
from datetime import datetime, timedelta

import pg
import datetime as dt

import src.lms_data_loader
from src import settings
from src.utils import debug


def dump_exists(path):
    return os.path.isfile(path) and os.path.isfile(path)


def dbconnect() -> pg.DB:
    return pg.DB(**settings.db_connection)


logger = logging.getLogger(settings.logger_name)


class Server:

    def __init__(self):
        logger.info("Connecting to DB")
        self._connection = dbconnect()
        logger.info("Connected")
        logger.info("Creating LMS loaders")
        self.student_loader = src.lms_data_loader.LmsStudentLoader(server=self)
        self.building_loader = src.lms_data_loader.LmsBuildingLoader(server=self)
        self.auditorium_loader = src.lms_data_loader.LmsAuditoriumLoader(self.building_loader, server=self)
        self.teacher_loader = src.lms_data_loader.LmsTeacherLoader(server=self)
        self.lesson_loader = src.lms_data_loader.LmsLessonLoader(self.auditorium_loader, self.teacher_loader,
                                                                 server=self)
        logger.info("Created")

    def get_user_info(self, user_id=None, user_name=None):
        if user_id:
            query = f"""SELECT * FROM students WHERE id = {user_id} LIMIT 1"""
        elif user_name:
            query = f"""SELECT * FROM find_users('{user_name}', '{user_name}', '{user_name}')"""
        else:
            raise KeyError("Neither user_id nor user_name are specified")

        result = self.get_simple_data(query, self.student_loader, user_name)

        return result

    def get_timetable(self, user_id, time_start=None, time_end=None):
        logger.debug(f"Entering with parameters user_id = {user_id}, time_start = {time_start}, time_end = {time_end}")
        if not time_start:
            time_start = datetime.now()
        if not time_end:
            time_end = datetime.now() + timedelta(days=365)
        query = f"""select *
                    from get_timetable_by_user_id({user_id}) timetable
                    where timetable.date between '{time_start}' and '{time_end}';"""
        result = self._connection.query(query).dictresult()
        logger.debug(result)
        if len(result) == 0:
            logger.debug("Not found: call LessonLoader")
            start = dt.datetime.strptime(time_start, "%d-%m-%Y").date()
            end = dt.datetime.strptime(time_end, "%d-%m-%Y").date()
            _result = self.lesson_loader.load_lessons(user_id, start, end)
            self.lesson_loader.add_to_db(lessons=_result)
            logger.debug("Retry query")
            result = self._connection.query(query).dictresult()
            logger.debug(result)
        return result

    def get_contingent_by_user_id(self, user_id):
        query = f"select * from get_contingent_id_by_user_id({user_id})"
        return self._connection.query(query).dictresult()

    def get_deadlines(self, user_id, time_start=None, time_end=None):
        if not time_start:
            time_start = datetime.now()
        if not time_end:
            time_end = datetime.now() + timedelta(days=365)
        logger.debug(f"Entering with parameters user_id = {user_id}, time_start = {time_start}, time_end = {time_end}")
        query = f"""select * from get_deadlines_by_id({user_id}, '{time_start}', '{time_end}')"""
        logger.debug(f"Sending query {query}")
        result = self._connection.query(query).dictresult()
        debug(result)
        return result

    def create_deadilne(self, user_id, contingent_id, time, weight, name, desc):
        debug(
            f"Entering with paraters user_id = {user_id}, time = {time}, weight = {weight}, name = {name}, desc = {desc}")
        query = f"""select insert_deadline({user_id}, {contingent_id}, '{time}', {weight}, '{name}', '{desc}')"""
        self._connection.query(query)
        res = self._connection.query("select lastval() as id").dictresult()[0]
        logger.debug(f"Inserted: {res}")
        return res

    def change_deadline_estimate(self, user_id, deadline_id, new_value):
        query = f"select id from task_time where student_id={user_id} and deadline_id={deadline_id}"
        logger.debug(f"Query {query}")
        task_ids = [x['id'] for x in self._connection.query(query).dictresult()]
        logger.debug(f"Ids {task_ids}")
        if len(task_ids) == 0:
            insert_query = f"insert into task_time (student_id, deadline_id, estimated_time, real_time) values ({user_id}, {deadline_id}, '{new_value} hours', null)"
            logger.debug(f"Insert: {insert_query}")
            self._connection.query(insert_query)
            task_ids = [x['id'] for x in self._connection.query(query).dictresult()]
            logger.debug(f"Ids {task_ids}")
        query = f"update task_time set estimated_time='{new_value} hours' where id in ({', '.join([str(x) for x in task_ids])})"
        logger.debug(f"Update: {query}")
        self._connection.query(query)

    def change_deadline_real(self, user_id, deadline_id, new_value):
        query = f"select id from task_time where student_id={user_id} and deadline_id={deadline_id}"
        logger.debug(f"Query {query}")
        task_ids = [x['id'] for x in self._connection.query(query).dictresult()]
        logger.debug(f"Ids {task_ids}")
        query = f"update task_time set real_time='{new_value} hours' where id in ({', '.join([str(x) for x in task_ids])})"
        logger.debug(f"Update: {query}")
        self._connection.query(query)
        pass

    def get_simple_data(self, query, lms_data_loader=None, term=None):
        logger.debug(f"Sending query {query}")

        result = self._connection.query(query).dictresult()

        logger.debug(f"Result: {result}")

        if len(result) == 0 and lms_data_loader is not None and term is not None:
            logger.debug(f"Got empty result, try use {type(lms_data_loader)} with term {term}")

            try:
                objs = lms_data_loader.load_term(term)
                logger.debug(f"Found {len(objs)} items: {objs}")
                lms_data_loader.add_to_db(objs)
                logger.debug("Saved to db")
                return [objs[key] for key in objs]
            except Exception as e:
                logger.debug(f"Something went wrong: {type(e)}: {e}")

        return result

    def get_building(self, id=None, building_name=None, building_addr=None):
        query = None
        term = None
        if id is not None:
            query = f"select * from buildings where id={id}"
        elif building_name is not None:
            value = "'%" + str(building_name) + "%'"
            query = f"select * from buildings where lower(name) like lower({value})"
            term = str(building_name)
        elif building_addr is not None:
            value = "'%" + str(building_addr) + "%'"
            query = f"select * from buildings where lower(addr) like lower({value})"
            term = str(building_addr)
        else:
            raise Exception('need more arguments')
        return self.get_simple_data(query, self.building_loader, term)

    def get_auditorium(self, id=None, number=None, building_id=None, building_name=None):
        key = None
        value = None
        term = None
        op = '='
        if id is not None:
            key = 'id'
            value = int(id)
        elif number is not None:
            key = 'number'
            value = "'%" + str(number) + "%'"
            term = str(number)
            op = ' like '
        else:
            raise Exception('need more arguments')

        query = f"select * from auditoriums where {key}{op}{value}"

        if id is None:
            if building_id is not None:
                building = self.get_building(building_id)
                if len(building) != 0:
                    query += f" and building_id={int(building_id)}"
                    term += ' | ' + building['name']
            elif building_name is not None:
                term += ' | ' + building_name

        return self.get_simple_data(query, self.auditorium_loader, term)

    def get_teacher(self, id=None, name=None, first_name=None, last_name=None, patronymic_name=None):
        query = None
        term = None
        if id is not None:
            query = f"select * from teachers where id={id}"
        elif name is not None:
            last_name, first_name, patronymic_name = self.teacher_loader.split_name(name)
            value = "'%" + str(last_name) + "%'"
            term = self.teacher_loader.join_names(last_name, first_name, patronymic_name)
            query = f"select * from teachers where lower(last_name) like lower({value})"
        else:
            raise Exception('need more arguments')
        return self.get_simple_data(query, self.teacher_loader, term)

    def get_learning_course(self, id=None, name=None):
        if id is not None:
            query = f"select * from learning_courses where id={id}"
        elif name is not None:
            value = "'%" + str(name) + "%'"
            query = f"select * from learning_courses where lower(shortname) like lower({value})"
        else:
            raise Exception('need more arguments')
        return self.get_simple_data(query, lms_data_loader=None, term=None)
