
create sequence h1_seq;

DROP TABLE H1;
CREATE TABLE H1 (
  H1_ID       number(9,0) PRIMARY KEY,
  INT1_K        number(5,0) NOT NULL,
  AUDIT_DTS   date,
  AUDIT_REC   varchar2(45),
  CONSTRAINT H1_UNIQUE UNIQUE (INT1_K)
) ;


create sequence h2_seq;

DROP TABLE H2;
CREATE TABLE H2 (
  H2_ID       number(9,0) PRIMARY KEY,
  INT2_K        number(5,0) NOT NULL,
  AUDIT_DTS   date,
  AUDIT_REC   varchar2(45),
  CONSTRAINT H2_UNIQUE UNIQUE (INT2_K)
) ;


DROP TABLE S2_1;
CREATE TABLE S2_1 (
  H2_ID number(9,0),
  string1 varchar2(1),
  string3 varchar2(3),
  ValidFrom date,
  ValidTo date,
  AUDIT_DTS   date,
  AUDIT_REC   varchar2(45),
  PRIMARY KEY (H2_ID,ValidFrom),
  FOREIGN KEY (H2_ID) REFERENCES H2 (H2_ID)
);

DROP TABLE S2_2;
CREATE TABLE S2_2 (
  H2_ID number(9,0),
  string1 varchar2(1),
  string3 varchar2(3),
  AUDIT_DTS   date,
  AUDIT_REC   varchar2(45),
  PRIMARY KEY (H2_ID),
  FOREIGN KEY (H2_ID) REFERENCES H2 (H2_ID)
);


create sequence h3_seq;

DROP TABLE H3;
CREATE TABLE H3 (
  H3_ID       number(9,0) PRIMARY KEY,
  INT3_K        number(5,0) NOT NULL,
  AUDIT_DTS   date,
  AUDIT_REC   varchar2(45),
  CONSTRAINT H3_UNIQUE UNIQUE (INT3_K)
) ;

create sequence h4_seq;

DROP TABLE H4;
CREATE TABLE H4 (
  H4_ID       number(9,0) PRIMARY KEY,
  INT1_K        number(5,0) NOT NULL,
  INT2_K        number(5,0) NOT NULL,
  INT3_K        number(5,0) NOT NULL,
  AUDIT_DTS   date,
  AUDIT_REC   varchar2(45),
  CONSTRAINT H4_UNIQUE UNIQUE (INT1_K,INT2_K,INT3_K)
) ;


create sequence h10_seq;

DROP TABLE H10;
CREATE TABLE H10 (
  H10_ID       number(9,0) PRIMARY KEY,
  INT10_K        number(9,0) NOT NULL,
  AUDIT_DTS   date,
  AUDIT_REC   varchar2(45),
  CONSTRAINT H10_UNIQUE UNIQUE (INT10_K)
) ;



DROP TABLE L123;
CREATE TABLE L123 (
  L123_ID number(9,0) PRIMARY KEY,
  H1_ID number(9,0) NOT NULL,
  H2_ID number(9,0) NOT NULL,
  H3_ID number(9,0) NOT NULL,
  AUDIT_DTS   date,
  AUDIT_REC   varchar2(45),
  CONSTRAINT LINK123_UNIQUE UNIQUE(H1_ID,H2_ID,H3_ID),
  FOREIGN KEY (H1_ID) REFERENCES H1 (H1_ID) ,
  FOREIGN KEY (H2_ID) REFERENCES H2 (H2_ID),
  FOREIGN KEY (H3_ID) REFERENCES H3 (H3_ID)
);


DROP TABLE S123_1;
CREATE TABLE S123_1 (
  L123_ID number(9,0),
  string5 varchar2(5),
  dec1_1 number(2,1),
  ValidFrom date,
  ValidTo date,
  AUDIT_DTS   date,
  AUDIT_REC   varchar2(45),
  PRIMARY KEY (L123_ID,ValidFrom),
  FOREIGN KEY (L123_ID) REFERENCES L123 (L123_ID)
);

DROP TABLE S123_2;
CREATE TABLE S123_2 (
  L123_ID number(9,0) NOT NULL,
  int_3 number(3,0),
  daterandom date,
  ValidFrom date NOT NULL,
  AUDIT_DTS   date,
  AUDIT_REC   varchar2(45),
  PRIMARY KEY (L123_ID,ValidFrom),
  FOREIGN KEY (L123_ID) REFERENCES L123 (L123_ID)
);



DROP TABLE L14_ai;
CREATE TABLE L14_ai (
  L14_ID number(9,0) NOT NULL,
  H1_ID number(9,0) NOT NULL,
  H4_ID number(9,0) NOT NULL,
  AUDIT_DTS   date,
  AUDIT_REC   varchar2(45),
  PRIMARY KEY (L14_ID),
  CONSTRAINT LINK14_UNIQUE UNIQUE (H1_ID,H4_ID),
  FOREIGN KEY (H1_ID) REFERENCES H1 (H1_ID),
  FOREIGN KEY (H4_ID) REFERENCES H4 (H4_ID)
);


select * from H1;
select * from H2;
select * from H3;
select * from H4;
select count(*) from H4;

select * from L123;
select count(*) from L123;
select * from L14_ai;
select count(*) from L14;

select * from S2_1;
select * from S2_2;
select * from S123_1
ORDER BY 1,4;
select * from S123_2
ORDER BY 1,4;

delete from S123_1;
delete from L123_ai;
delete from S2_1;
delete from S2_2;
delete from L14_ai;
delete from H4;
delete from H3;
delete from H2;
delete from H1;



