
 

 
 --DB CREATION PART - all the tables created hence not needed to re-run again. 

 CREATE TABLE teams 
(team_id INTEGER CHECK(player_id>0),
 PRIMARY KEY(team_id),
 ON DELETE CASCADE);



-- Depends on the creation of Teams first
 CREATE TABLE players 
(player_id INTEGER CHECK(player_id>0),
 team_id INTEGER,
 age INTEGER CHECK(age>0),
 height DECIMAL CHECK(height>0), 
 prefered_foot TEXT CHECK(UPPER(prefered_foot)='LEFT' OR UPPER(prefered_foot)='RIGHT'), 
 PRIMARY KEY(player_id),
 FOREIGN KEY(team_id) REFERENCES teams (team_id) ON DELETE CASCADE
);


-- Depends on the creation of Teams first
 CREATE TABLE stadiums
(stadium_id INTEGER CHECK(stadium_id>0),
 belongs_to_team INTEGER, 
 capacity INTEGER CHECK(capacity>0),
 PRIMARY KEY(stadium_id),
 UNIQUE (belongs_to_team),
 FOREIGN KEY(belongs_to_team) REFERENCES teams (team_id) ON DELETE CASCADE
 );






-- No need to add lines for Teams bc the Stadiums table exists. 

 

 

CREATE TABLE matches  
(match_id INTEGER CHECK(match_id>0),
 competition TEXT CHECK(UPPER(competition)='INTERNATIONAL' OR UPPER(competition)='DOMESTIC'),
 home_team_id INTEGER,
 away_team_id INTEGER CHECK(away_team_id != home_team_id),
 PRIMARY KEY(match_id),
 FOREIGN KEY(home_team_id) REFERENCES teams (team_id),
 FOREIGN KEY(away_team_id) REFERENCES teams (team_id)
 ON DELETE CASCADE);
 
 
CREATE TABLE goals  
(num_goals INTEGER check(num_goals >= 0),
 player_id INTEGER,
 match_id INTEGER,
 stadium_id INTEGER,
 PRIMARY KEY (num_goals,player_id,match_id), --redundent to add the stadium match_id is unique enought here
 FOREIGN KEY(player_id) REFERENCES players (player_id),
 FOREIGN KEY(stadium_id) REFERENCES stadiums (stadium_id),
 FOREIGN KEY(match_id) REFERENCES matches (match_id)
 ON DELETE CASCADE);
 
 
 CREATE TABLE spectators
(num_attendances INTEGER check(num_attendances >= 0),
 match_id INTEGER,
 stadium_id INTEGER,
 FOREIGN KEY(stadium_id) REFERENCES stadiums (stadium_id),
 FOREIGN KEY(match_id) REFERENCES matches (match_id)
 ON DELETE CASCADE); 





-- Part 3.4 Advanced API:


-- 3.4.1 - List<Int> getMostAttractiveStadiums()
/* Most attractive stadium is where the most goals were scored per its matches
Meaning, rate stadiums by there number of goals per all their Matches

 Help tables:
 5.	Goals: NumGoals, player_id, match_id , stadium_id, 
 3.	Stadium: Stadium_id (PK), Capacity, Belong_to(FK to Team table) 


Gets: <Nothing>
Returns: A list of most attractive stadium_ids

The list should be ordered by attractiveness in descending order, in case of equality order by ID in
ascending order.

List with the stadiums' IDs.
*Empty List in any other case.
*/

-- This is possible view for a result:

CREATE VIEW attractive_stadiums AS 
SELECT sum(num_goals) as attractiveness, stadium_id
FROM goals
GROUP BY stadium_id
-- HAVING attractiveness > 0;-- Maybe consult about this - but to remove stadiums that don't have any goals

-- Result: Can  use WHERE clause instead of Having in the view
SELECT stadium_id
FROM attractive_stadiums
WHERE attractiveness > 0 
ORDER BY attractiveness desc, stadium_id asc 



-----------------------------------------------------------------------------------
/*
--------------   3.4.2  ----------------------
List<Int> mostGoalsForTeam(Int teamID)
Returns a list of up to 5 players' IDs who scored the most goals for the team with
teamID.
The list should be ordered by:
• Main sort by the number of goals in descending order.
• Secondary sort by players' IDs in descending order.
Input: The teamID of the team in question.
Output:
*List with the players' IDs.
*Empty List in any other case.
 Note: only player who play for this team can be in this list.
*/

-- aux view:
CREATE VIEW top_players_view AS 
SELECT sum(g.NumGoals) as num_goals, g.player_id, t.team_id
FROM teams t INNER JOIN goals g USING (player_id)
GROUP BY g.player_id, t.team_id;

--Acutal answer: for the input teamID
SELECT player_id
FROM top_players_view
WHERE team_id = @teamID
ORDER BY num_goals desc, player_id desc
LIMIT 5

-- ****** Edge case if number of returned results is zero --> return empty list ***********
-------------------------------------------------       3.4.3        --------------------------------------------------
/*

List<Int> getClosePlayers (Int playerID)
Returns a list of the 10 "close players" to the player with playerID.
Close players are defined as players who scored in at least (>=) 50% of the matches the
player with playerID did. Note that a player cannot be a close player of itself.
The list should be ordered by IDs in ascending order.
Input: The ID of a player.
Output:
*List with the players' IDs that meet the conditions described above.
*Empty List in any other case.
 


 List<Int> getClosePlayers (Int playerID)

*/

-- AUX view
CREATE VIEW close_players_view AS
SELECT g1.player_id AS score_player, g2.player_id as close_players, COUNT(g1.player_id) as num_plays_of_scorer
FROM goals g1 INNER JOIN goals g2 USING (match_id)
WHERE g1.player_id != g2.player_id 
GROUP BY g1.player_id, g2.player_id
HAVING COUNT(g2.player_id) >= 0.5*COUNT(g1.player_id)


-- result query: 
SELECT close_players
FROM close_players_view
WHERE score_player = @playerID
ORDER BY player_id asc
LIMIT 10


-- Part 3.3 - basic API



/* 

Int stadiumTotalGoals(Int stadiumID)
Returns the total amount of goals scored in stadium with stadiumID.
Input: stadiumID of the requested stadium.
Output:
* The sum in case of success.
* 0 if the stadium does not exist,-1 in case of an error.

*/
SELECT SUM(num_goals)
FROM goals
WHERE stadium_id = @stadiumID
GROUP BY stadium_id



----***----
/*

Bool playerIsWinner(Int playerID, Int matchID)
Returns true if the player with playerID scored at least half of the goals in the match with
matchID.
For example:
The score was 4-2 for the player's team and the player scored 3 – Winner.
Input: player's ID and match's ID.
Output:
* True if playerID is a winner, False otherwise.
* False if the playerID or mathcID does not exist or no goals were scored in this match or in
case of an error.

*/

--Aux view - returns the match_id with the amount of goals scored there. 
CREATE VIEW total_goals_view AS 
SELECT SUM(NumGoals) sum_goals, match_id
FROM goals
GROUP BY match_id

SELECT g.player_id
FROM goals g INNER JOIN total_goals_view tg USING(match_id)
WHERE g.player_id = @playerID AND g.match_id = tg.match_id AND g.NumGoals >= (0.5*tg.sum_goals)


/*

List<Int> getActiveTallTeams()
Returns a List (up to size 5) of active teams' IDs that have at least 2 players over the height of 190cm.
Active team is a team who played at least 1 match at home or away (a record in Matches).
The list should be ordered by IDs in descending order.
Input: None.
Output:
* List with the teams' IDs.
* Empty List in any other case.
*/


CREATE VIEW active_teams_view AS
SELECT DISTINCT t.team_id AS active_team_id
FROM teams t
WHERE t.team_id IN (SELECT away_team_id FROM matches) OR t.team_id IN (SELECT home_team_id FROM matches)

--Aux view for the next question but will be used here as well
CREATE VIEW active_tall_teams_view AS 
SELECT p.team_id as team_id
FROM active_teams_view a INNER JOIN players p ON (a.active_team_id = p.team_id)
WHERE p.height >= 190
GROUP BY p.team_id 
HAVING COUNT(p.player_id) >= 2

--result
SELECT *
FROM active_tall_teams_view 
ORDER BY p.team_id desc
LIMIT 5

----------------------------------------------------------------------------------------------

/*

List<Int> getActiveTallRichTeams()
Returns a List (up to size 5) of active, rich teams' IDs that have at least 2 players over the height of
190cm. Active team is a team who played at least 1 match at home or away (a record in Matches).
Rich team is a team who owns a stadium of size >55,000.
The list should be ordered by IDs in ascending order.
Input: None.
Output:
* List with the teams' IDs.
* Empty List in any other case.

*/

CREATE VIEW rich_teams_view AS
SELECT t.team_id as team_id
FROM teams t INNER JOIN stadiums s ON (t.stadium = s.stadium_id)
WHERE s.capacity > 55000


SELECT DISTINCT a.team_id
FROM active_tall_teams_view a INNER JOIN rich_teams_view r USING (team_id)
ORDER BY a.team_id asc
LIMIT 5




 /*

List<Int> popularTeams()
Returns a List (up to size 10) of teams' IDs that in every single game they played as 'home team' they
had more than 40,000 attendance.
The list should be ordered by IDs in descending order.
Note: if a match did not take place in a specific stadium, treat it as if it had less than 40,000
attendance.
Input: None.
Output:
* List with the teams' IDs.
* Empty List in any other case

 */

-- Need to grab the team ids that played as 'Home'
-- and have more than 40k spectators in every-game they played. 

CREATE VIEW popular_matches_view AS
SELECT m.home_team_id 
FROM spectators s INNER JOIN matches m USING (match_id)
WHERE s.num_attendances > 40000 and s.stadium_id IS NOT NONE

CREATE VIEW not_popular_mathces_view AS 
SELECT m.home_team_id 
FROM spectators s INNER JOIN matches m USING (match_id)
WHERE s.num_attendances <= 40000 


CREATE_VIEW popular_teams_view as
SELECT home_team_id FROM popular_matches_view
EXCEPT
SELECT home_team_id FROM not_popular_mathces_view

SELECT team_id
FROM popular_teams_view 
ORDER BY team_id desc 
LIMIT 10  





CREATE VIEW home_matches AS 
SELECT t.team_id, m.match_id
FROM teams t INNER JOIN matches m ON (t.team_id = m.home_team_id)

SELECT team_id
FROM home_matches hm 
WHERE match_id





