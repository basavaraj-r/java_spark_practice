create table student (
	rollno serial not null,
	name varchar(50) not null,
	marks decimal not null,
	constraint student_pkey primary key (rollno),
	constraint student_uniquekey unique (name)
);

drop table student;
