CREATE DATABASE CRICKET;
show databases;
use cricket;

create table match_summary(
team1 varchar(15) ,
team2 varchar(15),
winner varchar(15),
margin varchar(15),
match_id varchar(15) ,
matchDay varchar(15) ,
primary key match_summary(match_id)
);
select * from match_summary;

LOAD DATA INFILE 'D:/datasets/dim_match_summary_Final.csv'
INTO TABLE match_summary
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 rows; 


alter table match_summary
add column Match_Day date after matchDay;

UPDATE match_summary
SET Match_Day = STR_TO_DATE(matchDay, '%m-%d-%Y');

select * from match_summary;
select winner , count(winner) as no_of_games_won from match_summary group by winner order by no_of_games_won desc;
alter table match_summary
drop column matchDay;
select * from match_summary;

select count(*) as Total_No_Of_records from match_summary;

CREATE TABLE dim_players (
	`name` VARCHAR(21) NOT NULL, 
	team VARCHAR(12) NOT NULL, 
	battingStyle VARCHAR(14) NOT NULL, 
	bowlingStyle VARCHAR(46) NOT NULL, 
	playingRole VARCHAR(19) NOT NULL,
    primary key dim_players(`name`)
);

load data infile
"D:/datasets/dim_players.csv"
into table dim_players
fields terminated by ','
enclosed by '"'
lines terminated by '\n'
ignore 1 rows;

select * from dim_players;

CREATE TABLE fact_bating_summary (
	match_id VARCHAR(7) NOT NULL, 
	`match` VARCHAR(28) NOT NULL, 
	teamInnings VARCHAR(12) NOT NULL, 
	battingPos varchar(15) NOT NULL, 
	batsmanName VARCHAR(18) NOT NULL, 
	`out/not_out` VARCHAR(7) NOT NULL, 
	runs int NOT NULL, 
	balls int NOT NULL, 
	`4s` int NOT NULL, 
	`6s` int NOT NULL, 
	`SR` DECIMAL(38, 0) NOT NULL,
    foreign key (match_id)  references match_summary(match_id),
    foreign key (batsmanName) references dim_players(`name`)
);

alter table fact_bating_summary
modify column `SR` decimal(40,2);


load data infile 
"D:/datasets/fact_bating_summary.csv"
into table  fact_bating_summary
fields terminated by ','
enclosed by '"'
lines terminated by '\n'
ignore 1 rows;

select * from fact_bating_summary;

CREATE TABLE fact_bowling_summary (
	match_id VARCHAR(7) NOT NULL, 
	`match` VARCHAR(28) NOT NULL, 
	bowlingTeam VARCHAR(12) NOT NULL, 
	bowlerName VARCHAR(21) NOT NULL, 
	overs INT NOT NULL, 
	maiden INT NOT NULL, 
	runs INT NOT NULL, 
	wickets INT NOT NULL, 
	economy INT NOT NULL, 
	`0s` INT NOT NULL, 
	`4s` INT NOT NULL, 
	`6s` INT NOT NULL, 
	wides INT NOT NULL, 
	`noBalls` INT NOT NULL,
    foreign key (match_id)  references match_summary(match_id),
    foreign key (bowlerName) references dim_players(`name`)
);

load data infile
'D:/datasets/fact_bowling_summary.csv'
into table fact_bowling_summary
fields terminated by ','
enclosed by '"'
lines terminated by '\n'
ignore 1 rows;

delimiter $$
create procedure playernames()
begin 
select * from dim_players;
end $$

call playernames();

delimiter $$ 
create procedure bowling()
begin 
select * from fact_bowling_summary;
end $$

call bowling();

delimiter $$
create procedure batting()
begin 
select * from  fact_bating_summary;
end $$

call batting();

delimiter &&
create procedure match_summary()
begin
select * from match_summary;
end &&

call match_summary();

-- Top 10 Runs scorer in last 3 years 

select batsmanName , sum(runs) as runs_scored from fact_bating_summary
 group by batsmanName 
 order by runs_scored desc
 limit 10;
 
 call batting();
 call match_summary();
 
 alter table match_summary
 add column `year` varchar(12) after Match_Day;
 
 update match_summary
 set `year` = year(Match_Day); 
 -- Top 10 batsmen based on past 3 years batting average. (min 60 balls faced ineach season)
 
 -- my method 
select T1.batsmanName ,T2.`year`, avg(T1.runs) as Average , sum(T1.balls) as Runs_scored from  fact_bating_summary as T1
left join
match_summary as T2
on T1.match_id = T2.match_id
group by T1.batsmanName , T2.`year`
having sum(T1.balls)>60
order by Average desc
limit 10;

-- copiolot method 
SELECT T1.BATSMANNAME, T2.`YEAR`, (SUM(T1.RUNS)/COUNT(T1.`OUT/NOT_OUT`)) AS BATTING_AVERAGE, SUM(T1.BALLS) AS BALLS_FACED
FROM FACT_BATING_SUMMARY AS T1
LEFT JOIN MATCH_SUMMARY AS T2
ON T1.MATCH_ID = T2.MATCH_ID
WHERE T1.`OUT/NOT_OUT` = "OUT"
GROUP BY T1.BATSMANNAME, T2.`YEAR`
HAVING SUM(T1.BALLS)>60
ORDER BY BATTING_AVERAGE DESC
LIMIT 10;


-- Top 10 batsmen based on past 3 years strike rate (min 60 balls faced in eachseason)

call batting();

SELECT T1.BATSMANNAME, T2.`YEAR`,AVG(T1.SR) AS STRIKERATE, SUM(T1.BALLS) AS BALLS_FACED
FROM FACT_BATING_SUMMARY AS T1
LEFT JOIN MATCH_SUMMARY AS T2
ON T1.MATCH_ID = T2.MATCH_ID
where T2.`year` between year(curdate())-3 and year(curdate()) 
GROUP BY T1.BATSMANNAME, T2.`YEAR`
HAVING SUM(T1.BALLS)>60
ORDER BY STRIKERATE DESC
LIMIT 10;

-- Top 10 bowlers based on past 3 years total wickets taken.
call bowling();

select bowlerName , sum(wickets) as Total_Wickets from fact_bowling_summary 
group by bowlerName
order by  Total_Wickets desc
limit 10;
-- Dynamic Method 

SELECT T1.BOWLERNAME, SUM(T1.WICKETS) AS TOTAL_WICKETS
FROM FACT_BOWLING_SUMMARY AS T1
LEFT JOIN MATCH_SUMMARY AS T2
ON T1.MATCH_ID = T2.MATCH_ID
WHERE T2.`YEAR` BETWEEN YEAR(CURDATE()) - 3 AND YEAR(CURDATE())
GROUP BY T1.BOWLERNAME
ORDER BY TOTAL_WICKETS DESC
LIMIT 10;

-- Top 10 bowlers based on past 3 years bowling average. (min 60 balls bowled in each season)

call bowling();

-- Bowling Avg = total runs conceded/total wickets taken

alter table  FACT_BOWLING_SUMMARY 
add column balls_bowled int after overs;

update FACT_BOWLING_SUMMARY
set balls_bowled = overs*6;

select T1.bowlerName as Bowler , sum(T1.runs)/sum(T1.wickets) as BowlingAverage from FACT_BOWLING_SUMMARY as T1
left join MATCH_SUMMARY as T2 
on T1.match_id = T2.match_id 
where T2.`year` between  year(curdate())-3 and year(curdate())
group by Bowler
having sum(balls_bowled)>=60
order by BowlingAverage 
limit 10;

-- Top 10 bowlers based on past 3 years economy rate. (min 60 balls bowled in each season)
call bowling();
select T1.bowlerName , avg(T1.economy) as Economy from FACT_BOWLING_SUMMARY as T1
left join match_summary as T2
on T1.match_id = T2.match_id
where T2.`year` between year(curdate())-3 and year(curdate())
group by T1.bowlerName 
having sum(T1.balls_bowled)>=60
order by Economy
limit 10;

-- Top 5 batsmen based on past 3 years boundary % (fours and sixes).
call batting();

alter table FACT_BATING_SUMMARY 
add column runs_scored_by_boundaries int after 6s;

update FACT_BATING_SUMMARY
set runs_scored_by_boundaries = 4s*4 + 6s*6;

-- boundary % = total runs scored by boundaries / total runs scored 

select T1.batsmanName, (sum(T1.runs_scored_by_boundaries)/sum( T1.runs))*100 as Boundary_Percentage from FACT_BATING_SUMMARY as T1
left join match_summary as  T2
on T1.match_id = T2.match_id 
where T2.`year` between year(curdate())-3 and year(curdate())
group by T1.batsmanName
order by Boundary_Percentage  desc
limit 5;


-- Top 5 bowlers based on past 3 years dot ball %
call batting();
alter table FACT_BATING_SUMMARY
drop  dot_ball_percentage;

call bowling();


alter table FACT_BOWLING_SUMMARY 
add column dot_ball_percentage decimal(10,2) after `6s`;

UPDATE FACT_BOWLING_SUMMARY
SET DOT_BALL_PERCENTAGE = (`0S`/nullif(BALLS_BOWLED,0))*100;


select T1.bowlerName , avg(T1.dot_ball_percentage) as dot_percentage  from FACT_BOWLING_SUMMARY as T1
left join match_summary as T2
on T1.match_id = T2.match_id 
where T2.`year` between year(curdate())-3 and year(curdate()) 
group by T1.bowlerName 
order by dot_percentage desc
limit 10;


-- Top 4 teams based on past 3 years winning %
call match_summary();

SELECT team1 as Team, 
       (SUM(CASE WHEN winner = team1 THEN 1 ELSE 0 END)/COUNT(*))*100 as Win_Percentage 
FROM match_summary
WHERE `year` BETWEEN YEAR(CURDATE())-3 AND YEAR(CURDATE())
GROUP BY team1
ORDER BY Win_Percentage DESC;

SELECT 
    team,
    (COUNT(*) / (SELECT COUNT(*) FROM match_summary WHERE team1 = m.team OR team2 = m.team)) * 100 AS win_percentage
FROM
    (SELECT team1 AS team, winner FROM match_summary
     UNION ALL
     SELECT team2 AS team, winner FROM match_summary) AS m
WHERE
    winner = team
GROUP BY
    team;





