--—оздание схемы и перемещение туда таблиц
--CREATE SCHEMA Dormitory 
--ALTER SCHEMA Dormitory TRANSFER [dbo].[rooms]
--ALTER SCHEMA Dormitory TRANSFER [dbo].[students]

SELECT * FROM Dormitory.rooms
SELECT * FROM Dormitory.students 


/* 1.—писок комнат и количество студентов в каждой из них
SELECT  r.name AS Rooms
	, COUNT (s.id) AS NumberOfStudents
FROM Dormitory.rooms r
JOIN Dormitory.students s ON s.room = r.id
GROUP BY s.room, r.name 
ORDER BY s.room*/


/* 2. 5 комнат, где самый маленький средний возраст студентов
SELECT TOP 5 r.name AS Rooms
	, AVG (DATEDIFF (YEAR, s.birthday, GETDATE())) AS AverageAge
FROM Dormitory.rooms r
JOIN Dormitory.students s ON s.room = r.id
GROUP BY r.name
ORDER BY AverageAge*/

/* 3. 5 комнат с самой большой разницей в возрасте студентов
SELECT TOP 5 r.name AS Rooms
	, MAX (DATEDIFF (YEAR, s.birthday, GETDATE()))- MIN(DATEDIFF (YEAR, s.birthday, GETDATE())) AS AgeDiff
FROM Dormitory.rooms r
JOIN Dormitory.students s ON s.room = r.id
GROUP BY r.name
ORDER BY AgeDiff DESC*/


/* 4. —писок комнат где живут разнополые студенты
SELECT r.name AS Rooms 
FROM Dormitory.rooms r
JOIN Dormitory.students s ON s.room = r.id
GROUP BY r.name, r.id
HAVING COUNT(DISTINCT s.sex) > 1
ORDER BY r.id*/

/* 5. ѕробовала оптимизировать с помощью индексов, но план выполнени€ практически не изменилс€ по показател€м, поэтому удалила их
CREATE INDEX idx_students_room ON Dormitory.students(room)
CREATE INDEX idx_rooms_id ON Dormitory.rooms(id)

DROP INDEX idx_students_room ON Dormitory.students
DROP INDEX idx_rooms_id ON Dormitory.rooms*/