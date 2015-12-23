create table yelp_businesses (
    business_id	    	varchar(50) primary key,
	business_name		varchar(100),
	business_state		char(3),
	business_city		varchar(50),
	business_zip		char(5),
	business_rating		number(*,2)
);

create table yelp_users (
	user_id			varchar(50) primary key,
	user_name		varchar(50)
);

create table reviews (
    review_id			varchar(50) primary key,
    rating				number(1) check ( rating >= 0 AND rating <= 5 ),
    author				varchar(50),
	business_id			varchar(50),
	review_text			blob,
	review_date			date,
    foreign key (author) references
	yelp_users (user_id),
	foreign key (business_id) references
	yelp_businesses (business_id)
);

create index review_business on reviews (business_id, review_id);

create table business_sub_category (
	business_id			varchar(50),
	main_category		varchar(5),
	sub_category		varchar(50),
	attributes			varchar(2000),
	foreign key (business_id) references yelp_businesses (business_id)
);

create table days_open (
	business_id			varchar(50) primary key,
	monday				char(1) default 'n' check ( monday in ('y','n') ) ,
	tuesday				char(1) default 'n' check ( tuesday in ('y','n') ),
	wednesday			char(1) default 'n' check ( wednesday in ('y','n') ),
	thursday			char(1) default 'n' check ( thursday in ('y','n') ),
	friday				char(1) default 'n' check ( friday in ('y','n') ),
	saturday			char(1) default 'n' check ( saturday in ('y','n') ),
	sunday				char(1) default 'n' check ( sunday in ('y','n') ),
	foreign key (business_id) references
	yelp_businesses (business_id)
);

