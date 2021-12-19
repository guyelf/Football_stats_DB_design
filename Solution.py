from typing import List
import Utility.DBConnector as Connector
from Utility.ReturnValue import ReturnValue
from Utility.Exceptions import DatabaseException
from Business.Match import Match
from Business.Player import Player
from Business.Stadium import Stadium
from psycopg2 import sql


# Leaving the CRUD part for the end when the dependence between the tables will be much clearer, to know in which order
# it's preferred to create them
# BASIC DESIGN IS PROVIDED IN THE DB SCHEMA THAT I CREATED
def createTables():
    try:
        conn = Connector.DBConnector()

        query = sql.SQL("""
                        CREATE TABLE teams (
                            team_id INTEGER CHECK(team_id>0),
                            PRIMARY KEY(team_id)
                        )
                        """)
        conn.execute(query)
        
        query = sql.SQL("""
                        CREATE TABLE players (
                            player_id INTEGER CHECK(player_id>0),
                            team_id INTEGER NOT NULL,
                            age INTEGER NOT NULL,
                            CHECK(age>0),
                            height DECIMAL NOT NULL,
                            CHECK(height>0), 
                            prefered_foot TEXT NOT NULL,
                            CHECK(UPPER(prefered_foot)='LEFT' OR UPPER(prefered_foot)='RIGHT'), 
                            PRIMARY KEY(player_id),
                            FOREIGN KEY(team_id) REFERENCES teams (team_id) ON DELETE CASCADE
                        )
                        """)
        conn.execute(query)

        query = sql.SQL("""
                        CREATE TABLE stadiums (
                            stadium_id INTEGER CHECK(stadium_id>0),
                            capacity INTEGER CHECK(capacity>0),
                            belongs_to_team INTEGER, 
                            PRIMARY KEY(stadium_id),
                            UNIQUE (belongs_to_team),
                            FOREIGN KEY(belongs_to_team) REFERENCES teams (team_id) ON DELETE CASCADE
                        )
                        """)
        conn.execute(query)

        query = sql.SQL("""
                        CREATE TABLE matches (
                            match_id INTEGER CHECK(match_id>0),
                            competition TEXT NOT NULL,
                            CHECK(UPPER(competition)='INTERNATIONAL' OR UPPER(competition)='DOMESTIC'),
                            home_team_id INTEGER NOT NULL,
                            away_team_id INTEGER NOT NULL,
                            CHECK(away_team_id != home_team_id),
                            PRIMARY KEY(match_id),
                            FOREIGN KEY(home_team_id) REFERENCES teams (team_id),
                            FOREIGN KEY(away_team_id) REFERENCES teams (team_id)
                            ON DELETE CASCADE
                        )
                        """)
        conn.execute(query)

        query = sql.SQL("""
                        CREATE TABLE goals (
                            num_goals INTEGER check(num_goals >= 0),
                            player_id INTEGER NOT NULL,
                            match_id INTEGER NOT NULL,
                            stadium_id INTEGER,
                            PRIMARY KEY (num_goals,player_id,match_id), --redundent to add the stadium match_id is unique enought here
                            FOREIGN KEY(player_id) REFERENCES players (player_id),
                            FOREIGN KEY(stadium_id) REFERENCES stadiums (stadium_id),
                            FOREIGN KEY(match_id) REFERENCES matches (match_id)
                            ON DELETE CASCADE
                        )
                        """)
        conn.execute(query)

        query = sql.SQL("""
                        CREATE TABLE spectators (
                            num_attendances INTEGER check(num_attendances >= 0),
                            match_id INTEGER NOT NULL,
                            stadium_id INTEGER,
                            FOREIGN KEY(stadium_id) REFERENCES stadiums (stadium_id),
                            FOREIGN KEY(match_id) REFERENCES matches (match_id)
                            ON DELETE CASCADE
                        )
                        """)
        conn.execute(query)



        # create all views

        query = sql.SQL("""
                        CREATE VIEW attractive_stadiums AS 
                            SELECT sum(num_goals) as attractiveness, stadium_id
                            FROM goals
                            GROUP BY stadium_id
                        """)
        conn.execute(query)

        query = sql.SQL("""
                        CREATE VIEW top_players_view AS 
                            SELECT SUM(g.num_goals) as num_goals, g.player_id, p.team_id
                            FROM players p INNER JOIN goals g USING (player_id)
                            GROUP BY p.team_id, g.player_id
                        """)
        conn.execute(query)

        query = sql.SQL("""
                        CREATE VIEW close_players_view AS
                            SELECT g1.player_id AS score_player, g2.player_id as close_players
                            FROM goals g1 INNER JOIN goals g2 USING (match_id)
                            WHERE g1.player_id != g2.player_id 
                            GROUP BY g1.player_id, g2.player_id
                            HAVING COUNT(g2.player_id) >= 0.5*COUNT(g1.player_id)
                        """)
        conn.execute(query)

        query = sql.SQL("""
                        CREATE VIEW total_goals_view AS 
                            SELECT SUM(num_goals) sum_goals, match_id
                            FROM goals
                            GROUP BY match_id
                        """)
        conn.execute(query)

        query = sql.SQL("""
                        CREATE VIEW active_teams_view AS
                            SELECT t.team_id AS active_team_id
                            FROM teams t
                            WHERE t.team_id IN (SELECT away_team_id FROM matches) OR t.team_id IN (SELECT home_team_id FROM matches)
                        """)
        conn.execute(query)

        query = sql.SQL("""
                        CREATE VIEW active_tall_teams_view AS 
                            SELECT p.team_id as team_id
                            FROM active_teams_view a INNER JOIN players p ON (a.active_team_id = p.team_id)
                            WHERE p.height >= 190
                            GROUP BY p.team_id 
                            HAVING COUNT(p.player_id) >= 2
                        """)
        conn.execute(query)

        query = sql.SQL("""
                        CREATE VIEW rich_teams_view AS
                            SELECT t.team_id as team_id
                            FROM teams t INNER JOIN stadiums s ON (t.team_id = s.belongs_to_team)
                            WHERE s.capacity > 55000
                        """)
        conn.execute(query)

        query = sql.SQL("""
                        CREATE VIEW popular_matches_view AS
                            SELECT m.home_team_id 
                            FROM spectators s INNER JOIN matches m USING (match_id)
                            WHERE s.num_attendances > 40000 and s.stadium_id IS NOT NULL
                        """)
        conn.execute(query)

        query = sql.SQL("""
                        CREATE VIEW not_popular_mathces_view AS 
                            SELECT m.home_team_id 
                            FROM spectators s INNER JOIN matches m USING (match_id)
                            WHERE s.num_attendances <= 40000 or s.stadium_id IS NULL
                        """)
        conn.execute(query)

    finally:
        conn.close()


def clearTables():
    try:
        conn = Connector.DBConnector()

        query = sql.SQL("""
                        TRUNCATE matches, players, stadiums, teams, goals, spectators CASCADE
                        """)
        conn.execute(query)
        
    finally:
        conn.close()


def dropTables():
    try:
        conn = Connector.DBConnector()

        # query = sql.SQL("""
        #                 DROP TABLE matches, players, stadiums, teams, goals, spectators CASCADE
        #                 """)
        query = sql.SQL("""                        
                        DROP SCHEMA public CASCADE;
                        CREATE SCHEMA public;
                        """)
        conn.execute(query)
        
    finally:
        conn.close()


# ####### ----------------------------------------------------------------------------
def addTeam(teamID: int) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("INSERT INTO teams(team_id) VALUES({teamID})").format(teamID=sql.Literal(teamID))
        rows_effected, _selected_rows = conn.execute(query)
        return ReturnValue.OK
    except DatabaseException.ConnectionInvalid as e:
        return ReturnValue.ERROR
    except DatabaseException.NOT_NULL_VIOLATION as e:
        return ReturnValue.BAD_PARAMS
    except DatabaseException.CHECK_VIOLATION as e:
        return ReturnValue.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION as e:
        return ReturnValue.ALREADY_EXISTS
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        return ReturnValue.BAD_PARAMS  # entered a bad stadium
    except Exception as e:
        print(e)
    finally:
        conn.close()


def addMatch(match: Match) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("INSERT INTO matches " +
                        "VALUES({matchID},{competition},{homeTeamID},{awayTeamID})").format(
            matchID=sql.Literal(match.getMatchID()),
            competition=sql.Literal(match.getCompetition()),
            homeTeamID=sql.Literal(match.getHomeTeamID()),
            awayTeamID=sql.Literal(match.getAwayTeamID()))
        rows_effected, _selected_rows = conn.execute(query)
        return ReturnValue.OK

    except DatabaseException.ConnectionInvalid as e:
        return ReturnValue.ERROR
    except DatabaseException.NOT_NULL_VIOLATION as e:
        return ReturnValue.BAD_PARAMS
    except DatabaseException.CHECK_VIOLATION as e:
        return ReturnValue.BAD_PARAMS
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        return ReturnValue.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION as e:
        return ReturnValue.ALREADY_EXISTS
    except Exception as e:
        print(e)
    finally:
        conn.close()


def getMatchProfile(matchID: int) -> Match:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT * " +
                        "FROM matches " +
                        "WHERE match_id = {matchID}").format(matchID=sql.Literal(matchID))
        rows_effected, _selected_rows = conn.execute(query)
        return Match(*_selected_rows[0])  # unpack of the tuple (should be only one)
    except Exception as e:
        return Match.badMatch()
    finally:
        conn.close()

# confirmed it's ok to use IF on rows_effected based on:
# todo: https://piazza.com/class/kqz4dh15z2p1m1?cid=74

def deleteMatch(match: Match) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("DELETE FROM matches " +
                        "WHERE match_id = {matchID}").format(matchID=sql.Literal(match.getMatchID()))
        rows_effected, _selected_rows = conn.execute(query)
        if rows_effected == 0:
            return ReturnValue.NOT_EXISTS
        else:
            return ReturnValue.OK
    except DatabaseException.ConnectionInvalid as e:
        return ReturnValue.ERROR
    except Exception as e:
        print(e)
    finally:
        conn.close()


def addPlayer(player: Player) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("INSERT INTO players " +
                        "VALUES({playerID},{teamId},{age},{height},{foot})").format(
            playerID=sql.Literal(player.getPlayerID()),
            teamId=sql.Literal(player.getTeamID()),
            age=sql.Literal(player.getAge()),
            height=sql.Literal(player.getHeight()),
            foot=sql.Literal(player.getFoot()))
        rows_effected, _selected_rows = conn.execute(query)

        return ReturnValue.OK
    except DatabaseException.ConnectionInvalid as e:
        return ReturnValue.ERROR
    except DatabaseException.UNIQUE_VIOLATION as e:
        return ReturnValue.ALREADY_EXISTS
    except DatabaseException.NOT_NULL_VIOLATION as e:
        return ReturnValue.BAD_PARAMS
    except DatabaseException.CHECK_VIOLATION as e:
        return ReturnValue.BAD_PARAMS
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        return ReturnValue.BAD_PARAMS
    except Exception as e:
        print(e)
    finally:
        conn.close()


def getPlayerProfile(playerID: int) -> Player:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT * " +
                        "FROM players " +
                        "WHERE player_id = {playerID}").format(playerID=sql.Literal(playerID))
        rows_effected, _selected_rows = conn.execute(query)

        if len(_selected_rows.rows) == 0:  # No rows were fetched for that player.
            return ReturnValue.NOT_EXISTS
        else:
            return Player(*_selected_rows[0])  # unpack of the tuple (should be only one)

    except Exception as e:
        return Player.badPlayer()
    finally:
        conn.close()


def deletePlayer(player: Player) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("DELETE FROM players " +
                        "WHERE player_id = {playerID}").format(playerID=sql.Literal(player.getPlayerID()))
        rows_effected, _selected_rows = conn.execute(query)
        if rows_effected == 0:
            return ReturnValue.NOT_EXISTS
        else:
            return ReturnValue.OK

    except DatabaseException.ConnectionInvalid as e:
        return ReturnValue.ERROR
    except Exception as e:
        print(e)
    finally:
        conn.close()


def addStadium(stadium: Stadium) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("INSERT INTO stadiums " +
                        "VALUES({stadiumID},{capacity},{belongsTo})").format(
            stadiumID=sql.Literal(stadium.getStadiumID()),
            capacity=sql.Literal(stadium.getCapacity()),
            belongsTo=sql.Literal(stadium.getBelongsTo())
        )
        rows_effected, _selected_rows = conn.execute(query)
        return ReturnValue.OK

    except DatabaseException.ConnectionInvalid as e:
        return ReturnValue.ERROR
    except DatabaseException.UNIQUE_VIOLATION as e:
        return ReturnValue.ALREADY_EXISTS
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        return ReturnValue.BAD_PARAMS
    except DatabaseException.NOT_NULL_VIOLATION as e:
        return ReturnValue.BAD_PARAMS
    except DatabaseException.CHECK_VIOLATION as e:
        return ReturnValue.BAD_PARAMS
    except Exception as e:
        print(e)
    finally:
        conn.close()


def getStadiumProfile(stadiumID: int) -> Stadium:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT * " +
                        "FROM stadiums " +
                        "WHERE stadium_id = {stadiumID}").format(stadiumID=sql.Literal(stadiumID))
        rows_effected, _selected_rows = conn.execute(query)

        if len(_selected_rows) == 0: # in case the stadium doesn't exist
            return Stadium.badStadium()
        else:
            return Stadium(*_selected_rows[0])  # unpack of the tuple (should be only one - id is pk)

    except Exception as e:
        return Stadium.badStadium()
    finally:
        conn.close()


def deleteStadium(stadium: Stadium) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("DELETE FROM stadiums " +
                        "WHERE stadium_id = {stadiumID}").format(stadiumID=sql.Literal(stadium.getStadiumID()))
        rows_effected, _selected_rows = conn.execute(query)
        if rows_effected == 0:
            return ReturnValue.NOT_EXISTS
        else:
            return ReturnValue.OK

    except DatabaseException.ConnectionInvalid as e:
        return ReturnValue.ERROR
    except Exception as e:
        print(e)
    finally:
        conn.close()

# Part 3.3 Implementation - Basic API:
#todo: How & Where to keep this data - which tables

def playerScoredInMatch(match: Match, player: Player, amount: int) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("""
                        INSERT INTO Goals
                        VALUES({amount}, {player}, {match},
                            (
                            SELECT stadium_id
                            FROM Spectators
                            WHERE match_id = {match}
                            )
                        )
                        """).format(player=sql.Literal(player.getPlayerID()), match=sql.Literal(match.getMatchID()), amount=sql.Literal(amount))
        conn.execute(query)

        return ReturnValue.OK

    except DatabaseException.CHECK_VIOLATION as e:
        return ReturnValue.BAD_PARAMS
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        return ReturnValue.NOT_EXISTS
    except DatabaseException.UNIQUE_VIOLATION as e:
        return ReturnValue.ALREADY_EXISTS
    except Exception as e:
        return ReturnValue.ERROR
    finally:
        conn.close()


def playerDidntScoreInMatch(match: Match, player: Player) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("""
                        DELETE FROM Goals
                        WHERE player_id={player} AND match_id={match}
                        """).format(player=sql.Literal(player.getPlayerID()), match=sql.Literal(match.getMatchID()))
        rows_affected, _ = conn.execute(query)

        if rows_affected == 0:
            return ReturnValue.NOT_EXISTS
        else:
            return ReturnValue.OK


    except Exception as e:
        return ReturnValue.ERROR 
    finally:
        conn.close()



def matchInStadium(match: Match, stadium: Stadium, attendance: int) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("""
                        INSERT INTO Spectators
                        VALUES({attendance}, {match}, {stadium})
                        """).format(attendance=sql.Literal(attendance), match=sql.Literal(match.getMatchID()), stadium=sql.Literal(stadium.getStadiumID()))
        conn.execute(query)

        return ReturnValue.OK


    except DatabaseException.CHECK_VIOLATION as e:
        return ReturnValue.BAD_PARAMS
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        return ReturnValue.NOT_EXISTS
    except DatabaseException.UNIQUE_VIOLATION as e:
        return ReturnValue.ALREADY_EXISTS
    except Exception as e:
        return ReturnValue.ERROR
    finally:
        conn.close()


def matchNotInStadium(match: Match, stadium: Stadium) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("""
                        DELETE FROM Spectators
                        WHERE match_id={match} AND stadium_id={stadium}
                        """).format(match=sql.Literal(match.getMatchID()), stadium=sql.Literal(stadium.getStadiumID()))
        rows_affected, _ = conn.execute(query)

        if rows_affected == 0:
            return ReturnValue.NOT_EXISTS
        else:
            return ReturnValue.OK

    except Exception as e:
        return ReturnValue.ERROR 
    finally:
        conn.close()


def averageAttendanceInStadium(stadiumID: int) -> float:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("""
                        SELECT AVG(numAttendance) FROM Spectators
                        WHERE stadium_id={stadium}
                        GROUP BY stadium_id
                        """).format(stadium=sql.Literal(stadiumID))
        rows_affected, output = conn.execute(query)

        if rows_affected == 0:
            return 0
        else:
            return output[0][0]

    except Exception as e:
        return -1

    finally:
        conn.close()


def stadiumTotalGoals(stadiumID: int) -> int:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT SUM(num_goals) " +
                        "FROM goals " +
                        "WHERE stadium_id = {stadiumID} " +
                        "GROUP BY stadium_id ").format(stadiumID=sql.Literal(stadiumID))
        rows_effected, _selected_rows = conn.execute(query)

        if len(_selected_rows) == 0:
            return 0
        else:
            return _selected_rows.rows[0][0] # should be one value

    except Exception as e:
        return -1
    finally:
        conn.close()


def playerIsWinner(playerID: int, matchID: int) -> bool:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT g.player_id " +
                        "FROM goals g INNER JOIN total_goals_view tg USING(match_id)" +
                        "WHERE g.player_id = {playerID} AND g.match_id = {matchID} AND g.NumGoals >= (0.5*tg.sum_goals) ").format(matchID=sql.Literal(matchID), playerID=sql.Literal(playerID))
        rows_effected, _selected_rows = conn.execute(query)

        if len(_selected_rows) == 0:
            return False
        else:
            return _selected_rows.rows[0][0]  # should be one value --> player_id =! 0 --> which is equals to TRUE

    except Exception as e:
        return False
    finally:
        conn.close()


def getActiveTallTeams() -> List[int]:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT team_id " +
                        "FROM active_tall_teams_view  " +
                        "ORDER BY p.team_id desc " +
                        "LIMIT 5 ")
        rows_effected, _selected_rows = conn.execute(query)
        return _selected_rows.rows # should return a list

    except Exception as e:
        return []
    finally:
        conn.close()


def getActiveTallRichTeams() -> List[int]:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT DISTINCT a.team_id " +
                        "FROM active_tall_teams_view a INNER JOIN rich_teams_view r USING (team_id) " +
                        "ORDER BY a.team_id asc " +
                        "LIMIT 5  ")
        rows_effected, _selected_rows = conn.execute(query)
        return _selected_rows.rows  # should return a list

    except Exception as e:
        return []
    finally:
        conn.close()


def popularTeams() -> List[int]:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT team_id " +
                        "FROM popular_teams_view  " +
                        "ORDER BY team_id desc " +
                        "LIMIT 10 ")
        rows_effected, _selected_rows = conn.execute(query)
        return _selected_rows.rows  # should return a list

    except Exception as e:
        return []
    finally:
        conn.close()

# 3.4 Advanced API - done

def getMostAttractiveStadiums() -> List[int]:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT stadium_id " +
                        "FROM attractive_stadiums_view " +
                        "WHERE attractiveness > 0 " +
                        "ORDER BY attractiveness desc, stadium_id asc ")
        rows_effected, _selected_rows = conn.execute(query)
        return _selected_rows.rows # Should flat the list of tuples? - This will require more code in python

    except Exception as e:
        return []
    finally:
        conn.close()


def mostGoalsForTeam(teamID: int) -> List[int]:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT player_id " +
                        "FROM top_players_view " +
                        "WHERE team_id = {teamID} " +
                        "ORDER BY num_goals desc, player_id desc " +
                        "LIMIT 5 ").format(teamID=sql.Literal(teamID))
        rows_effected, _selected_rows = conn.execute(query)
        return _selected_rows.rows

    except Exception as e:
        return []
    finally:
        conn.close()


def getClosePlayers(playerID: int) -> List[int]:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT close_players " +
                        "FROM close_players_view " +
                        "WHERE score_player = {playerID} "
                        "ORDER BY close_players asc " +
                        "LIMIT 10 ").format(playerID=sql.Literal(playerID))
        rows_effected, _selected_rows = conn.execute(query)
        return _selected_rows.rows

    except Exception as e:
        return []
    finally:
        conn.close()


if __name__ == '__main__':
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT first_name FROM guy_test WHERE person_id < 10")
        rows_affected, _selected_rows = conn.execute(query=query)
    except Exception as e:
        print(e)
    finally:
        conn.close()
        print("This is the #rows selected:" + _selected_rows)
        #print(rows_affected)
