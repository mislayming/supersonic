-------S2VisitsDemo
CREATE TABLE IF NOT EXISTS `s2_user_department` (
    `user_name` varchar(200) NOT NULL,
    `department` varchar(200) NOT NULL, -- department of user
     PRIMARY KEY (`user_name`,`department`)
    );
COMMENT ON TABLE s2_user_department IS 'user_department_info';

CREATE TABLE IF NOT EXISTS `s2_pv_uv_statis` (
    `imp_date` varchar(200) NOT NULL,
    `user_name` varchar(200) NOT NULL,
    `page` varchar(200) NOT NULL
    );
COMMENT ON TABLE s2_pv_uv_statis IS 's2_pv_uv_statis';

CREATE TABLE IF NOT EXISTS `s2_stay_time_statis` (
    `imp_date` varchar(200) NOT NULL,
    `user_name` varchar(200) NOT NULL,
    `stay_hours` DOUBLE NOT NULL,
    `page` varchar(200) NOT NULL
    );
COMMENT ON TABLE s2_stay_time_statis IS 's2_stay_time_statis_info';

-------S2ArtistDemo
CREATE TABLE IF NOT EXISTS `singer` (
    `singer_name` varchar(200) NOT NULL,
    `act_area` varchar(200) NOT NULL,
    `song_name` varchar(200) NOT NULL,
    `genre` varchar(200) NOT NULL,
    `js_play_cnt` bigINT DEFAULT NULL,
    `down_cnt` bigINT DEFAULT NULL,
    `favor_cnt` bigINT DEFAULT NULL,
     PRIMARY KEY (`singer_name`)
    );
COMMENT ON TABLE singer IS 'singer_info';

CREATE TABLE IF NOT EXISTS `genre` (
    `g_name` varchar(20) NOT NULL , -- genre name
    `rating` INT ,
    `most_popular_in` varchar(50) ,
    PRIMARY KEY (`g_name`)
    );
COMMENT ON TABLE genre IS 'genre';

-------S2CompanyDemo
CREATE TABLE IF NOT EXISTS `company` (
    `company_id` varchar(50) NOT NULL ,
    `company_name` varchar(50) NOT NULL ,
    `headquarter_address` varchar(50) NOT NULL ,
    `company_established_time` varchar(20) NOT NULL ,
    `founder` varchar(20) NOT NULL ,
    `ceo` varchar(20) NOT NULL ,
    `annual_turnover` bigint  ,
    `employee_count` int ,
    PRIMARY KEY (`company_id`)
    );

CREATE TABLE IF NOT EXISTS `brand` (
    `brand_id` varchar(50) NOT NULL ,
    `brand_name` varchar(50) NOT NULL ,
    `brand_established_time` varchar(20) NOT NULL ,
    `company_id` varchar(50) NOT NULL ,
    `legal_representative` varchar(20) NOT NULL ,
    `registered_capital` bigint  ,
    PRIMARY KEY (`brand_id`)
    );

CREATE TABLE IF NOT EXISTS `brand_revenue` (
    `year_time` varchar(10) NOT NULL ,
    `brand_id` varchar(50) NOT NULL ,
    `revenue` bigint NOT NULL,
    `profit` bigint NOT NULL ,
    `revenue_growth_year_on_year` double NOT NULL ,
    `profit_growth_year_on_year` double NOT NULL
    );

CREATE TABLE  IF NOT EXISTS  `tv_channel` (
      "id" TEXT NOT NULL,
      "series_name" TEXT DEFAULT NULL,
      "Country" TEXT DEFAULT NULL,
      "Language" TEXT DEFAULT NULL,
      "Content" TEXT DEFAULT NULL,
      "Pixel_aspect_ratio_PAR" TEXT DEFAULT NULL,
      "Hight_definition_TV" TEXT DEFAULT NULL,
      "Pay_per_view_PPV" TEXT DEFAULT NULL,
      "Package_Option" TEXT DEFAULT NULL,
      PRIMARY KEY ("id")
);

CREATE TABLE IF NOT EXISTS   "cartoon" (
   "id" REAL NOT NULL,
   "Title" TEXT DEFAULT NULL,
   "Directed_by" TEXT DEFAULT NULL,
   "Written_by" TEXT DEFAULT NULL,
   "Original_air_date" TEXT DEFAULT NULL,
   "Production_code" REAL DEFAULT NULL,
   "Channel" TEXT DEFAULT NULL,
   PRIMARY KEY ("id"),
   FOREIGN KEY ("Channel") REFERENCES "tv_channel" ("id")
);

CREATE TABLE IF NOT EXISTS  "tv_series" (
     "id" REAL NOT NULL,
     "Episode" TEXT DEFAULT NULL,
     "Air_Date" TEXT DEFAULT NULL,
     "Rating" TEXT DEFAULT NULL,
     "Share" REAL DEFAULT NULL,
     "18_49_Rating_Share" TEXT DEFAULT NULL,
     "Viewers_m" TEXT DEFAULT NULL,
     "Weekly_Rank" REAL DEFAULT NULL,
     "Channel" TEXT DEFAULT NULL,
     PRIMARY KEY ("id"),
     FOREIGN KEY ("Channel") REFERENCES "tv_channel" ("id")
);
