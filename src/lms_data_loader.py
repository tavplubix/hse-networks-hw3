import datetime as dt
import json
import logging
import pprint

import requests
import transliterate as tr

import src.server_backend
import src.settings as settings

logger = logging.getLogger(settings.logger_name)


class DataLoader:
    def __init__(self):
        pass


class LmsDataLoader(DataLoader):
    url = 'https://ruz.hse.ru/api/search'
    answ_len = 15

    def __init__(self, objtype, table, alphabet, max_depth=4, server=None):
        super(LmsDataLoader, self).__init__()
        self.alphabet = alphabet
        self.requrl = LmsDataLoader.url + '?type=' + objtype + '&term='
        self.max_depth = max_depth
        self.server = server
        if self.server is None:
            self.db = src.server_backend.dbconnect()
        else:
            self.db = self.server._connection
        self.table = table
        self.objects = {}

    def normalize_obj(self, obj):
        obj.pop('type')

    def data(self):
        return self.objects

    def load_term(self, term, save=True):
        r = requests.get(self.requrl + term, verify=False)
        if r.status_code != 200:
            raise Exception('RUZ is down')
        objs = json.loads(r.content)
        obj_dict = {}
        for obj in objs:
            id = obj['id']
            self.normalize_obj(obj)
            obj_dict[id] = obj
        if save:
            self.objects = {**self.objects, **obj_dict}
        return obj_dict

    def load_terms(self, terms, save=True):
        all = {}
        for term in terms:
            all = {**all, **self.load_term(term, False)}
        if save:
            self.objects = {**self.objects, **all}
        return all

    def load_all_tree(self, prefix='', depth=0):
        if self.max_depth < depth:
            return
        objs = self.load_term(prefix, False)
        print('loaded prefix', prefix, 'size', len(objs))
        self.objects = {**self.objects, **objs}
        if len(objs) < LmsDataLoader.answ_len:
            return
        for c in self.alphabet:
            self.load_all_tree(prefix + c, depth + 1)

    def add_to_db(self, objects=None):
        logger.debug(f"{objects}")
        objs_to_add = objects
        if objs_to_add is None:
            objs_to_add = self.objects
        print(f"""\n\nTRY ADD {type(self)} TO DB: {len(objects)}\n\n""")
        for key in objs_to_add:
            obj = objs_to_add[key]
            self.db.upsert(self.table, obj)


class LmsBuildingLoader(LmsDataLoader):

    def __init__(self, server=None):
        super(LmsBuildingLoader, self).__init__('building', 'buildings', list('йцукенгшщзхъфывапролджэячсмитьбю'),
                                                server=server)

    def normalize_obj(self, obj):
        super(LmsBuildingLoader, self).normalize_obj(obj)
        obj['name'] = obj['label']
        obj.pop('label')
        obj['addr'] = obj['description']
        obj.pop('description')

    def get_building_id(self, name):
        for key in self.objects:
            if self.objects[key]['name'] == name:
                return key
        new = self.load_terms([name, ])
        for key in new:
            if new[key]['name'] == name:
                return key
        return None


class LmsAuditoriumLoader(LmsDataLoader):

    def __init__(self, building_loader, server=None):
        super(LmsAuditoriumLoader, self).__init__('auditorium', 'auditoriums', list('0123456789'), max_depth=3,
                                                  server=server)
        self.building_loader = building_loader

    def normalize_obj(self, obj):
        super(LmsAuditoriumLoader, self).normalize_obj(obj)
        descr = obj['description'].split(' | ')
        obj.pop('description')
        obj['number'] = obj['label']
        obj.pop('label')
        obj['building_id'] = self.server.get_building(building_name=descr[1])[0]['id']
        obj['auditorium_type'] = descr[2]


class LmsPersonLoader(LmsDataLoader):
    def __init__(self, objtype, table, server=None):
        super(LmsPersonLoader, self).__init__(objtype, table, list('йцукенгшщзхъфывапролджэячсмитьбю'), server=server)
        self.email_domain = None

    def normalize_obj(self, obj):
        super(LmsPersonLoader, self).normalize_obj(obj)
        name = self.split_name(obj['label'])
        obj.pop('label')
        obj['last_name'] = name[0]
        obj['first_name'] = name[1]
        obj['patronymic_name'] = name[2]
        obj['email'] = self.make_email(name[0], name[1], name[2])

    def split_name(self, full_name):
        names = full_name.split(' ')
        names += [None, ] * (3 - len(names))
        return names

    def join_names(self, last_name='', first_name='', patronymic_name=''):
        return ' '.join([x for x in [last_name, first_name, patronymic_name] if x is not None])

    def load_by_name(self, last_name='', first_name='', patronymic_name=''):
        return self.load_terms(self.join_names(last_name, first_name, patronymic_name))

    def make_email(self, last_name, first_name, patronymic_name):
        if last_name is None or first_name is None or patronymic_name is None:
            return None
        if len(last_name) == 0 or len(first_name) == 0 or len(patronymic_name) == 0:
            return None
        if self.email_domain is None:
            return None
        fn = tr.translit(first_name, 'ru', reversed=True)
        pn = tr.translit(patronymic_name, 'ru', reversed=True)
        ln = tr.translit(last_name, 'ru', reversed=True)
        return (fn[0] + pn[0] + ln + '@' + self.email_domain).lower().replace('-', '').replace("'", '')


class LmsTeacherLoader(LmsPersonLoader):

    def __init__(self, server=None):
        super(LmsTeacherLoader, self).__init__('person', 'teachers', server=server)
        self.email_domain = 'hse.ru'

    def normalize_obj(self, obj):
        super(LmsTeacherLoader, self).normalize_obj(obj)
        obj['department'] = obj['description']
        obj.pop('description')


class LmsStudentLoader(LmsPersonLoader):
    def __init__(self, server=None):
        super(LmsStudentLoader, self).__init__('student', 'students', server=server)
        self.email_domain = 'edu.hse.ru'

    def normalize_obj(self, obj):
        super(LmsStudentLoader, self).normalize_obj(obj)
        obj['group_name'] = obj['description']
        obj.pop('description')


class LmsLessonLoader:
    url = 'https://ruz.hse.ru/api/schedule/student/'

    def __init__(self, auditoriumLoader, teacherLoader, server=None):
        self.server = server
        if self.server is None:
            self.db = src.server_backend.dbconnect()
        else:
            self.db = self.server._connection
        self.auditoriumLoader = auditoriumLoader
        self.teacherLoader = teacherLoader
        self.table = 'lesson'
        self.lessons = {}

    def normalize_lesson(self, lesson):
        norm = {}
        norm['id'] = lesson['date'] + str(lesson['lessonNumberEnd'])
        norm['lesson_time_id'] = lesson['lessonNumberStart']
        norm['auditorium_id'] = lesson['auditoriumOid']
        norm['auditorium'] = lesson['auditorium'].split('/')[-1].strip()
        norm['building'] = lesson['building'].strip()
        norm['course_id'] = lesson['disciplineOid']
        norm['course'] = lesson['discipline'].strip()
        norm['contingent_id'] = lesson['streamOid']
        norm['contingent'] = lesson['stream'].strip()
        norm['teacher'] = ' '.join(lesson['lecturer'].split(' ')[1:]).strip()
        norm['lesson_type'] = lesson['kindOfWork'].strip()
        norm['date'] = dt.datetime.strptime(lesson['date'], "%Y.%m.%d").date()
        norm['id'] = str(norm['date']) + ' ' + str(norm['lesson_time_id'])
        return norm

    def link_lesson(self, lesson, student_id):

        if len(self.server.get_auditorium(id=lesson['auditorium_id'])) == 0:
            self.server.get_auditorium(number=lesson['auditorium'], building_name=lesson['building'])
        lesson.pop('auditorium')
        lesson.pop('building')

        course = self.db.query(f"""SELECT * FROM learning_courses WHERE id={lesson['course_id']}""").dictresult()
        if len(course) == 0:
            self.db.insert('learning_courses',
                           {'id': lesson['course_id'], 'shortname': lesson['course'], 'fullname': lesson['course']})
        lesson.pop('course')

        cont = self.db.query(f"""SELECT * FROM contingents WHERE id={lesson['contingent_id']}""").dictresult()
        if len(cont) == 0:
            self.db.insert('contingents', {'id': lesson['contingent_id'], 'contingent_name': lesson['contingent']})
        lesson.pop('contingent')

        self.db.query(f"""INSERT INTO students_to_contingents VALUES ({student_id}, {lesson[
            'contingent_id']}) ON CONFLICT DO NOTHING""")

        teachers = self.server.get_teacher(name=lesson['teacher']);
        if len(teachers) == 0:
            lesson['teacher_id'] = None
        else:
            lesson['teacher_id'] = list(teachers)[0]['id']
        lesson.pop('teacher')

        lesson.pop('id')
        return lesson

    def data(self):
        return self.lessons

    def load_lessons(self, student_id, begin, end=None, save=True):
        pp = pprint.PrettyPrinter(indent=4, width=140)
        student = self.db.query(f"""SELECT * FROM students WHERE id={student_id}""").dictresult()
        if len(student) == 0:
            raise Exception("student_id " + str(student_id) + " not found in DB, load student first")
        if end is None:
            end = begin
        requrl = LmsLessonLoader.url + str(student_id)
        params = {'start': begin.strftime("%Y.%m.%d"), 'end': end.strftime("%Y.%m.%d"), 'lng': 1}
        r = requests.get(requrl, params=params, verify=False)
        if r.status_code != 200:
            raise Exception('RUZ is down')
        lessons = json.loads(r.content)
        lessons_dict = {}
        for lesson in lessons:
            id = lesson['date'] + str(lesson['lessonNumberEnd'])
            lesson = self.normalize_lesson(lesson)
            print('\n\nNORMALIZED LESSON:')
            pp.pprint(lesson)
            lesson = self.link_lesson(lesson, student_id)
            print('\n\nLINKED LESSON:')
            pp.pprint(lesson)
            lessons_dict[id] = lesson
        if save:
            self.lessons = {**self.lessons, **lessons_dict}
        return lessons_dict

    def add_to_db(self, lessons=None):
        l = lessons
        if l is None:
            l = self.lessons
        print(f"""\n\nTRY ADD LESSONS TO DB: {len(lessons)}\n\n""")
        for key in l:
            obj = l[key]
            self.db.upsert(self.table, obj)


def test_loader():
    pp = pprint.PrettyPrinter(indent=4, width=140)

    server = src.server_backend.Server()

    student_loader = LmsStudentLoader(server=server)
    s = student_loader.load_terms(['Токмаков, Иванов, Петров'])
    pp.pprint(s)
    student_loader.add_to_db()
    print('\n\n\n')

    building_lodaer = LmsBuildingLoader(server=server)
    audirotium_loader = LmsAuditoriumLoader(building_lodaer, server=server)
    teacher_loader = LmsTeacherLoader(server=server)

    lesson_loader = LmsLessonLoader(audirotium_loader, teacher_loader, server=server)

    l = lesson_loader.load_lessons(136782, dt.datetime.strptime('2019.06.09', "%Y.%m.%d").date(),
                                   dt.datetime.strptime('2019.06.15', "%Y.%m.%d").date())
    pp.pprint(l)
    print('\n\n\n')

    building_lodaer.add_to_db()
    audirotium_loader.add_to_db()
    teacher_loader.add_to_db()

    lesson_loader.add_to_db()


if __name__ == '__main__':
    test_loader()
