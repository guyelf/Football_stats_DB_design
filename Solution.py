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
    pass


def clearTables():
    pass


def dropTables():
    pass


# ####### ----------------------------------------------------------------------------
def addTeam(teamID: int) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("INSERT INTO teams(team_id) VALUES({teamID})").format(teamID=sql.Literal(teamID))
        rows_effected, _selected_rows = conn.execute(query)
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
        return ReturnValue.OK


def addMatch(match: Match) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("INSERT INTO matches " +
                        "VALUES({matchID,competition,homeTeamID,awayTeamID})").format(
            matchID=sql.Literal(match.getMatchID()),
            competition=sql.Literal(match.getCompetition()),
            homeTeamID=sql.Literal(match.getHomeTeamID()),
            awayTeamID=sql.Literal(match.getAwayTeamID()))
        rows_effected, _selected_rows = conn.execute(query)
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
        return ReturnValue.OK


def getMatchProfile(matchID: int) -> Match:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT * " +
                        "FROM matches " +
                        "WHERE match_id = {matchID}").format(matchID=sql.Literal(matchID))
        rows_effected, _selected_rows = conn.execute(query)
    except Exception as e:
        return Match.badMatch()
    finally:
        conn.close()
        return Match(*_selected_rows[0])  # unpack of the tuple (should be only one)

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
    except DatabaseException.ConnectionInvalid as e:
        return ReturnValue.ERROR
    except Exception as e:
        print(e)
    finally:
        conn.close()
        return ReturnValue.OK


def addPlayer(player: Player) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("INSERT INTO players " +
                        "VALUES({playerID,teamId,age,height,foot})").format(
            playerID=sql.Literal(player.getPlayerID()),
            teamId=sql.Literal(player.getTeamID()),
            age=sql.Literal(player.getAge()),
            height=sql.Literal(player.getHeight()),
            foot=sql.Literal(player.getFoot()))
        rows_effected, _selected_rows = conn.execute(query)
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
        return ReturnValue.OK


def getPlayerProfile(playerID: int) -> Player:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT * " +
                        "FROM players " +
                        "WHERE player_id = {playerID}").format(playerID=sql.Literal(playerID))
        rows_effected, _selected_rows = conn.execute(query)
    except Exception as e:
        return Player.badPlayer()
    finally:
        conn.close()
        return Player(*_selected_rows[0])  # unpack of the tuple (should be only one)


def deletePlayer(player: Player) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("DELETE FROM players " +
                        "WHERE player_id = {playerID}").format(playerID=sql.Literal(player.getPlayerID()))
        rows_effected, _selected_rows = conn.execute(query)
        if rows_effected == 0:
            return ReturnValue.NOT_EXISTS
    except DatabaseException.ConnectionInvalid as e:
        return ReturnValue.ERROR
    except Exception as e:
        print(e)
    finally:
        conn.close()
        return ReturnValue.OK


def addStadium(stadium: Stadium) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("INSERT INTO stadiums " +
                        "VALUES({stadiumID,capacity,belongsTo})").format(
            stadiumID=sql.Literal(Stadium.getStadiumID()),
            capacity=sql.Literal(Stadium.getCapacity()),
            belongsTo=sql.Literal(Stadium.getBelongsTo())
        )
        rows_effected, _selected_rows = conn.execute(query)
    except DatabaseException.ConnectionInvalid as e:
        return ReturnValue.ERROR
    except DatabaseException.UNIQUE_VIOLATION as e:
        return ReturnValue.ALREADY_EXISTS
# This is different than the others (Players/Matches) to enforce the case where a team has already owned the suggested stadium
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        return ReturnValue.ALREADY_EXISTS

    except DatabaseException.NOT_NULL_VIOLATION as e:
        return ReturnValue.BAD_PARAMS
    except DatabaseException.CHECK_VIOLATION as e:
        return ReturnValue.BAD_PARAMS
    except Exception as e:
        print(e)
    finally:
        conn.close()
        return ReturnValue.OK


def getStadiumProfile(stadiumID: int) -> Stadium:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT * " +
                        "FROM stadiums " +
                        "WHERE stadium_id = {stadiumID}").format(stadiumID=sql.Literal(stadiumID))
        rows_effected, _selected_rows = conn.execute(query)
    except Exception as e:
        return Stadium.badStadium()
    finally:
        conn.close()
        return Stadium(*_selected_rows[0])  # unpack of the tuple (should be only one - id is pk)


def deleteStadium(stadium: Stadium) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("DELETE FROM stadiums " +
                        "WHERE stadium_id = {stadiumID}").format(stadiumID=sql.Literal(stadium.getStadiumID()))
        rows_effected, _selected_rows = conn.execute(query)
        if rows_effected == 0:
            return ReturnValue.NOT_EXISTS
    except DatabaseException.ConnectionInvalid as e:
        return ReturnValue.ERROR
    except Exception as e:
        print(e)
    finally:
        conn.close()
        return ReturnValue.OK


def playerScoredInMatch(match: Match, player: Player, amount: int) -> ReturnValue:
    pass


def playerDidntScoreInMatch(match: Match, player: Player) -> ReturnValue:
    pass


def matchInStadium(match: Match, stadium: Stadium, attendance: int) -> ReturnValue:
    pass


def matchNotInStadium(match: Match, stadium: Stadium) -> ReturnValue:
    pass


def averageAttendanceInStadium(stadiumID: int) -> float:
    pass


def stadiumTotalGoals(stadiumID: int) -> int:
    pass


def playerIsWinner(playerID: int, matchID: int) -> bool:
    pass


def getActiveTallTeams() -> List[int]:
    pass


def getActiveTallRichTeams() -> List[int]:
    pass


def popularTeams() -> List[int]:
    pass


def getMostAttractiveStadiums() -> List[int]:
    pass


def mostGoalsForTeam(teamID: int) -> List[int]:
    pass


def getClosePlayers(playerID: int) -> List[int]:
    pass


if __name__ == '__main__':
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT * FROM guy_test")
        rows_affected, _selected_rows = conn.execute(query=query)
    except Exception as e:
        print(e)
    finally:
        conn.close()
        print(_selected_rows)
        print(rows_affected)
