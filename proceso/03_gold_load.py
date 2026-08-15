# Databricks notebook source
# Databricks notebook source
# ============================================================
# FIFA World Cup 2026 Analytics Platform
# Gold Layer
# SILVER Delta Tables -> GOLD Analytical Delta Tables
# ============================================================

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------
# Widgets

dbutils.widgets.text(
    "environment",
    "dev"
)

dbutils.widgets.text(
    "storage_account",
    "stworldcupanalytics"
)

environment = dbutils.widgets.get("environment")
storage_account = dbutils.widgets.get("storage_account")

catalog_name = f"worldcup_{environment}"

gold_path = (
    f"abfss://gold-{environment}@"
    f"{storage_account}.dfs.core.windows.net/"
)

print(f"Environment : {environment}")
print(f"Catalog     : {catalog_name}")
print(f"GOLD Path   : {gold_path}")

# COMMAND ----------
# Validate Silver Tables

display(
    spark.sql(
        f"""
        SHOW TABLES IN {catalog_name}.silver
        """
    )
)

# COMMAND ----------
# Read Silver Tables

df_standard = spark.table(
    f"{catalog_name}.silver.player_standard"
)

df_shooting = spark.table(
    f"{catalog_name}.silver.player_shooting"
)

df_playingtime = spark.table(
    f"{catalog_name}.silver.player_playing_time"
)

df_misc = spark.table(
    f"{catalog_name}.silver.player_miscellaneous"
)

df_goalkeeping = spark.table(
    f"{catalog_name}.silver.player_goalkeeping"
)

df_matches = spark.table(
    f"{catalog_name}.silver.match_fixtures"
)

print("Silver tables loaded successfully.")

print("Player Standard:", df_standard.count())
print("Player Shooting:", df_shooting.count())
print("Player Playing Time:", df_playingtime.count())
print("Player Miscellaneous:", df_misc.count())
print("Player Goalkeeping:", df_goalkeeping.count())
print("Match Fixtures:", df_matches.count())

# COMMAND ----------
# Prepare Fact Player Statistics

shooting_metrics = (
    df_shooting
        .select(
            "PlayerId",
            "TotalShots",
            "ShotsOnTarget",
            "ShotsOnTargetPercentage",
            "ShotsPer90",
            "ShotsOnTargetPer90",
            "GoalsPerShot",
            "GoalsPerShotOnTarget"
        )
)

playingtime_metrics = (
    df_playingtime
        .select(
            "PlayerId",
            "AverageMinutesPerMatch",
            "TeamMinutesPercentage",
            "AverageMinutesPerStart",
            "CompleteMatches",
            "SubstituteAppearances",
            "AverageMinutesAsSubstitute",
            "UnusedSubstituteMatches",
            "PointsPerMatch",
            "GoalsForWhileOnField",
            "GoalsAgainstWhileOnField",
            "GoalDifference",
            "GoalDifferencePer90",
            "OnOffGoalDifference"
        )
)

misc_metrics = (
    df_misc
        .select(
            "PlayerId",
            "SecondYellowRedCards",
            "FoulsCommitted",
            "FoulsReceived",
            "Offsides",
            "Crosses",
            "Interceptions",
            "TacklesWon",
            "PenaltiesWon",
            "PenaltiesConceded",
            "OwnGoals"
        )
)

fact_player_statistics = (
    df_standard
        .join(
            shooting_metrics,
            "PlayerId",
            "left"
        )
        .join(
            playingtime_metrics,
            "PlayerId",
            "left"
        )
        .join(
            misc_metrics,
            "PlayerId",
            "left"
        )
        .withColumn(
            "GoldCreatedDate",
            current_timestamp()
        )
)

display(fact_player_statistics.limit(10))

(
    fact_player_statistics.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .option(
            "path",
            f"{gold_path}fact_player_statistics"
        )
        .saveAsTable(
            f"{catalog_name}.gold.fact_player_statistics"
        )
)

print("Gold table created: fact_player_statistics")

# COMMAND ----------
# Prepare Gold Base

fact_players = spark.table(
    f"{catalog_name}.gold.fact_player_statistics"
)

fact_players_gold_base = (
    fact_players
        .withColumn("Goals", col("Goals").cast("int"))
        .withColumn("Assists", col("Assists").cast("int"))
        .withColumn("GoalsAssists", col("GoalsAssists").cast("int"))
        .withColumn("GoalsWithoutPenalty", col("GoalsWithoutPenalty").cast("int"))
        .withColumn("PenaltyGoals", col("PenaltyGoals").cast("int"))
        .withColumn("PenaltyAttempts", col("PenaltyAttempts").cast("int"))

        .withColumn("YellowCards", col("YellowCards").cast("int"))
        .withColumn("RedCards", col("RedCards").cast("int"))

        .withColumn("GoalsPer90", col("GoalsPer90").cast("double"))
        .withColumn("AssistsPer90", col("AssistsPer90").cast("double"))
        .withColumn("GoalsAssistsPer90", col("GoalsAssistsPer90").cast("double"))

        .withColumn("TotalShots", col("TotalShots").cast("int"))
        .withColumn("ShotsOnTarget", col("ShotsOnTarget").cast("int"))
        .withColumn("ShotsOnTargetPercentage", col("ShotsOnTargetPercentage").cast("double"))
        .withColumn("ShotsPer90", col("ShotsPer90").cast("double"))
        .withColumn("ShotsOnTargetPer90", col("ShotsOnTargetPer90").cast("double"))
        .withColumn("GoalsPerShot", col("GoalsPerShot").cast("double"))
        .withColumn("GoalsPerShotOnTarget", col("GoalsPerShotOnTarget").cast("double"))

        .withColumn("MinutesPlayed", col("MinutesPlayed").cast("int"))
        .withColumn("MatchesPlayed", col("MatchesPlayed").cast("int"))
        .withColumn("MatchesStarted", col("MatchesStarted").cast("int"))
        .withColumn("Equivalent90MinMatches", col("Equivalent90MinMatches").cast("double"))

        .withColumn("SecondYellowRedCards", col("SecondYellowRedCards").cast("int"))
        .withColumn("FoulsCommitted", col("FoulsCommitted").cast("int"))
        .withColumn("FoulsReceived", col("FoulsReceived").cast("int"))
        .withColumn("Offsides", col("Offsides").cast("int"))
        .withColumn("Crosses", col("Crosses").cast("int"))
        .withColumn("Interceptions", col("Interceptions").cast("int"))
        .withColumn("TacklesWon", col("TacklesWon").cast("int"))
        .withColumn("PenaltiesWon", col("PenaltiesWon").cast("int"))
        .withColumn("PenaltiesConceded", col("PenaltiesConceded").cast("int"))
        .withColumn("OwnGoals", col("OwnGoals").cast("int"))
)

# COMMAND ----------
# Gold Top Scorers

gold_top_scorers = (
    fact_players_gold_base
        .select(
            "PlayerId",
            "PlayerName",
            "PlayerPosition",
            "NationalTeam",
            "PlayerClub",
            "Goals",
            "Assists",
            "GoalsAssists",
            "PenaltyGoals",
            "TotalShots",
            "ShotsOnTarget",
            "ShotsOnTargetPercentage",
            "GoalsPer90",
            "GoalsPerShot",
            "MinutesPlayed"
        )
        .filter(
            col("Goals").isNotNull()
        )
        .orderBy(
            col("Goals").desc(),
            col("Assists").desc(),
            col("GoalsPer90").desc()
        )
        .withColumn(
            "GoldCreatedDate",
            current_timestamp()
        )
)

display(gold_top_scorers.limit(10))

(
    gold_top_scorers.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .option(
            "path",
            f"{gold_path}top_scorers"
        )
        .saveAsTable(
            f"{catalog_name}.gold.top_scorers"
        )
)

print("Gold table created: top_scorers")

# COMMAND ----------
# Gold Top Assists

gold_top_assists = (
    fact_players_gold_base
        .select(
            "PlayerId",
            "PlayerName",
            "PlayerPosition",
            "NationalTeam",
            "PlayerClub",
            "Assists",
            "Goals",
            "GoalsAssists",
            "AssistsPer90",
            "MinutesPlayed"
        )
        .filter(
            col("Assists").isNotNull()
        )
        .orderBy(
            col("Assists").desc(),
            col("AssistsPer90").desc()
        )
        .withColumn(
            "GoldCreatedDate",
            current_timestamp()
        )
)

display(gold_top_assists.limit(10))

(
    gold_top_assists.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .option(
            "path",
            f"{gold_path}top_assists"
        )
        .saveAsTable(
            f"{catalog_name}.gold.top_assists"
        )
)

print("Gold table created: top_assists")

# COMMAND ----------
# Gold Player Offensive Ranking

gold_player_offensive_ranking = (
    fact_players_gold_base
        .select(
            "PlayerId",
            "PlayerName",
            "PlayerPosition",
            "NationalTeam",
            "PlayerClub",
            "Goals",
            "Assists",
            "GoalsAssists",
            "TotalShots",
            "ShotsOnTarget",
            "ShotsOnTargetPercentage",
            "GoalsPer90",
            "AssistsPer90",
            "GoalsAssistsPer90",
            "MinutesPlayed"
        )
        .withColumn(
            "OffensiveScore",
            (
                coalesce(col("Goals"), lit(0)) * lit(4) +
                coalesce(col("Assists"), lit(0)) * lit(3) +
                coalesce(col("ShotsOnTarget"), lit(0)) * lit(1)
            )
        )
        .orderBy(
            col("OffensiveScore").desc()
        )
        .withColumn(
            "GoldCreatedDate",
            current_timestamp()
        )
)

display(gold_player_offensive_ranking.limit(10))

(
    gold_player_offensive_ranking.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .option(
            "path",
            f"{gold_path}player_offensive_ranking"
        )
        .saveAsTable(
            f"{catalog_name}.gold.player_offensive_ranking"
        )
)

print("Gold table created: player_offensive_ranking")

# COMMAND ----------
# Gold Player Defensive and Discipline

gold_player_defensive_discipline = (
    fact_players_gold_base
        .select(
            "PlayerId",
            "PlayerName",
            "PlayerPosition",
            "NationalTeam",
            "PlayerClub",
            "YellowCards",
            "RedCards",
            "SecondYellowRedCards",
            "FoulsCommitted",
            "FoulsReceived",
            "Interceptions",
            "TacklesWon",
            "OwnGoals",
            "MinutesPlayed"
        )
        .withColumn(
            "DefensiveActions",
            coalesce(col("Interceptions"), lit(0)) +
            coalesce(col("TacklesWon"), lit(0))
        )
        .withColumn(
            "DisciplineRiskScore",
            coalesce(col("YellowCards"), lit(0)) +
            coalesce(col("SecondYellowRedCards"), lit(0)) * lit(2) +
            coalesce(col("RedCards"), lit(0)) * lit(3) +
            coalesce(col("FoulsCommitted"), lit(0))
        )
        .orderBy(
            col("DefensiveActions").desc(),
            col("DisciplineRiskScore").asc()
        )
        .withColumn(
            "GoldCreatedDate",
            current_timestamp()
        )
)

display(gold_player_defensive_discipline.limit(10))

(
    gold_player_defensive_discipline.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .option(
            "path",
            f"{gold_path}player_defensive_discipline"
        )
        .saveAsTable(
            f"{catalog_name}.gold.player_defensive_discipline"
        )
)

print("Gold table created: player_defensive_discipline")

# COMMAND ----------
# Prepare Goalkeeping Gold Base

goalkeeping_gold_base = (
    df_goalkeeping
        .withColumn("MatchesPlayed", col("MatchesPlayed").cast("int"))
        .withColumn("MatchesStarted", col("MatchesStarted").cast("int"))
        .withColumn("MinutesPlayed", col("MinutesPlayed").cast("int"))
        .withColumn("Equivalent90MinMatches", col("Equivalent90MinMatches").cast("double"))
        .withColumn("GoalsAgainst", col("GoalsAgainst").cast("int"))
        .withColumn("GoalsAgainstPer90", col("GoalsAgainstPer90").cast("double"))
        .withColumn("ShotsOnTargetAgainst", col("ShotsOnTargetAgainst").cast("int"))
        .withColumn("Saves", col("Saves").cast("int"))
        .withColumn("SavePercentage", col("SavePercentage").cast("double"))
        .withColumn("Wins", col("Wins").cast("int"))
        .withColumn("Draws", col("Draws").cast("int"))
        .withColumn("Losses", col("Losses").cast("int"))
        .withColumn("CleanSheets", col("CleanSheets").cast("int"))
        .withColumn("CleanSheetPercentage", col("CleanSheetPercentage").cast("double"))
        .withColumn("PenaltyAttemptsAgainst", col("PenaltyAttemptsAgainst").cast("int"))
        .withColumn("PenaltyGoalsAgainst", col("PenaltyGoalsAgainst").cast("int"))
        .withColumn("PenaltySaves", col("PenaltySaves").cast("int"))
        .withColumn("PenaltyMissesAgainst", col("PenaltyMissesAgainst").cast("int"))
        .withColumn("PenaltySavePercentage", col("PenaltySavePercentage").cast("double"))
)

# COMMAND ----------
# Gold Goalkeeper Ranking

gold_goalkeeper_ranking = (
    goalkeeping_gold_base
        .select(
            "PlayerId",
            "PlayerName",
            "NationalTeam",
            "PlayerClub",
            "MatchesPlayed",
            "MinutesPlayed",
            "GoalsAgainst",
            "GoalsAgainstPer90",
            "ShotsOnTargetAgainst",
            "Saves",
            "SavePercentage",
            "CleanSheets",
            "CleanSheetPercentage",
            "PenaltySaves",
            "PenaltySavePercentage"
        )
        .withColumn(
            "GoalkeeperScore",
            (
                coalesce(col("Saves"), lit(0)) * lit(2) +
                coalesce(col("CleanSheets"), lit(0)) * lit(5) +
                coalesce(col("SavePercentage"), lit(0)) / lit(10)
            )
        )
        .orderBy(
            col("GoalkeeperScore").desc(),
            col("SavePercentage").desc()
        )
        .withColumn(
            "GoldCreatedDate",
            current_timestamp()
        )
)

display(gold_goalkeeper_ranking.limit(10))

(
    gold_goalkeeper_ranking.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .option(
            "path",
            f"{gold_path}goalkeeper_ranking"
        )
        .saveAsTable(
            f"{catalog_name}.gold.goalkeeper_ranking"
        )
)

print("Gold table created: goalkeeper_ranking")

# COMMAND ----------
# Team Normalization Functions

def extract_team_code(team_col):
    return (
        when(
            regexp_extract(team_col, r"^([a-z]{2,3})\s+", 1) != "",
            regexp_extract(team_col, r"^([a-z]{2,3})\s+", 1)
        )
        .otherwise(
            regexp_extract(team_col, r"\s+([a-z]{2,3})$", 1)
        )
    )


def extract_team_name(team_col):
    return (
        when(
            regexp_extract(team_col, r"^([a-z]{2,3})\s+", 1) != "",
            trim(
                regexp_replace(
                    team_col,
                    r"^[a-z]{2,3}\s+",
                    ""
                )
            )
        )
        .otherwise(
            trim(
                regexp_replace(
                    team_col,
                    r"\s+[a-z]{2,3}$",
                    ""
                )
            )
        )
    )

# COMMAND ----------
# Prepare Matches Gold Base

matches_gold_base = (
    df_matches
        .filter(col("HomeTeam").isNotNull())
        .filter(col("AwayTeam").isNotNull())
        .filter(col("FinalScore").isNotNull())
        .withColumn(
            "MatchDate",
            col("MatchDate").cast("date")
        )
        .withColumn(
            "Attendance",
            col("Attendance").cast("int")
        )
        .withColumn(
            "HomeTeamCode",
            lower(
                extract_team_code(
                    col("HomeTeam")
                )
            )
        )
        .withColumn(
            "AwayTeamCode",
            lower(
                extract_team_code(
                    col("AwayTeam")
                )
            )
        )
        .withColumn(
            "HomeTeamName",
            extract_team_name(
                col("HomeTeam")
            )
        )
        .withColumn(
            "AwayTeamName",
            extract_team_name(
                col("AwayTeam")
            )
        )
        .withColumn(
            "CleanScore",
            trim(
                regexp_replace(
                    col("FinalScore"),
                    r"\([^)]*\)",
                    ""
                )
            )
        )
        .withColumn(
            "HomeGoals",
            trim(
                split(
                    col("CleanScore"),
                    "[–-]"
                ).getItem(0)
            ).cast("int")
        )
        .withColumn(
            "AwayGoals",
            trim(
                split(
                    col("CleanScore"),
                    "[–-]"
                ).getItem(1)
            ).cast("int")
        )
        .withColumn(
            "MatchResult",
            when(
                col("HomeGoals") > col("AwayGoals"),
                lit("Home Win")
            )
            .when(
                col("HomeGoals") < col("AwayGoals"),
                lit("Away Win")
            )
            .otherwise(
                lit("Draw")
            )
        )
)

display(matches_gold_base.limit(10))

# COMMAND ----------
# Home Team Results

home_team_results = (
    matches_gold_base
        .select(
            col("TournamentRound"),
            col("MatchDate"),
            col("HomeTeamName").alias("TeamName"),
            col("HomeTeamCode").alias("TeamCode"),
            col("AwayTeamName").alias("OpponentTeamName"),
            col("AwayTeamCode").alias("OpponentTeamCode"),
            col("HomeGoals").alias("GoalsFor"),
            col("AwayGoals").alias("GoalsAgainst"),
            col("Attendance"),
            col("Venue"),
            col("Referee"),
            col("FinalScore")
        )
        .withColumn(
            "Result",
            when(
                col("GoalsFor") > col("GoalsAgainst"),
                lit("Win")
            )
            .when(
                col("GoalsFor") < col("GoalsAgainst"),
                lit("Loss")
            )
            .otherwise(
                lit("Draw")
            )
        )
)

# COMMAND ----------
# Away Team Results

away_team_results = (
    matches_gold_base
        .select(
            col("TournamentRound"),
            col("MatchDate"),
            col("AwayTeamName").alias("TeamName"),
            col("AwayTeamCode").alias("TeamCode"),
            col("HomeTeamName").alias("OpponentTeamName"),
            col("HomeTeamCode").alias("OpponentTeamCode"),
            col("AwayGoals").alias("GoalsFor"),
            col("HomeGoals").alias("GoalsAgainst"),
            col("Attendance"),
            col("Venue"),
            col("Referee"),
            col("FinalScore")
        )
        .withColumn(
            "Result",
            when(
                col("GoalsFor") > col("GoalsAgainst"),
                lit("Win")
            )
            .when(
                col("GoalsFor") < col("GoalsAgainst"),
                lit("Loss")
            )
            .otherwise(
                lit("Draw")
            )
        )
)

team_match_results = (
    home_team_results
        .unionByName(
            away_team_results
        )
)

# COMMAND ----------
# Gold Team Performance

gold_team_performance = (
    team_match_results
        .groupBy(
            "TeamName",
            "TeamCode"
        )
        .agg(
            count("*").alias("MatchesPlayed"),
            sum(
                when(
                    col("Result") == "Win",
                    1
                )
                .otherwise(0)
            ).alias("Wins"),
            sum(
                when(
                    col("Result") == "Draw",
                    1
                )
                .otherwise(0)
            ).alias("Draws"),
            sum(
                when(
                    col("Result") == "Loss",
                    1
                )
                .otherwise(0)
            ).alias("Losses"),
            sum("GoalsFor").alias("GoalsFor"),
            sum("GoalsAgainst").alias("GoalsAgainst"),
            avg("GoalsFor").alias("AverageGoalsFor"),
            avg("GoalsAgainst").alias("AverageGoalsAgainst")
        )
        .withColumn(
            "GoalDifference",
            col("GoalsFor") - col("GoalsAgainst")
        )
        .withColumn(
            "Points",
            col("Wins") * lit(3) + col("Draws")
        )
        .orderBy(
            col("Points").desc(),
            col("GoalDifference").desc(),
            col("GoalsFor").desc()
        )
        .withColumn(
            "GoldCreatedDate",
            current_timestamp()
        )
)

display(gold_team_performance.limit(20))

(
    gold_team_performance.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .option(
            "path",
            f"{gold_path}team_performance"
        )
        .saveAsTable(
            f"{catalog_name}.gold.team_performance"
        )
)

print("Gold table created: team_performance")

# COMMAND ----------
# Gold Tournament Summary

gold_tournament_summary = (
    matches_gold_base
        .agg(
            count("*").alias("TotalMatches"),
            sum("HomeGoals").alias("TotalHomeGoals"),
            sum("AwayGoals").alias("TotalAwayGoals"),
            sum("Attendance").alias("TotalAttendance"),
            avg("Attendance").alias("AverageAttendance")
        )
        .withColumn(
            "TotalGoals",
            col("TotalHomeGoals") + col("TotalAwayGoals")
        )
        .withColumn(
            "AverageGoalsPerMatch",
            col("TotalGoals") / col("TotalMatches")
        )
        .withColumn(
            "GoldCreatedDate",
            current_timestamp()
        )
)

display(gold_tournament_summary)

(
    gold_tournament_summary.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .option(
            "path",
            f"{gold_path}tournament_summary"
        )
        .saveAsTable(
            f"{catalog_name}.gold.tournament_summary"
        )
)

print("Gold table created: tournament_summary")

# COMMAND ----------
# Gold Match Details

gold_match_details = (
    matches_gold_base
        .select(
            "TournamentRound",
            "TournamentWeek",
            "MatchDay",
            "MatchDate",
            "MatchTime",
            "HomeTeamName",
            "HomeTeamCode",
            "AwayTeamName",
            "AwayTeamCode",
            "FinalScore",
            "HomeGoals",
            "AwayGoals",
            "MatchResult",
            "Attendance",
            "Venue",
            "Referee",
            "MatchNotes"
        )
        .withColumn(
            "GoldCreatedDate",
            current_timestamp()
        )
)

display(gold_match_details.limit(20))

(
    gold_match_details.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .option(
            "path",
            f"{gold_path}match_details"
        )
        .saveAsTable(
            f"{catalog_name}.gold.match_details"
        )
)

print("Gold table created: match_details")

# COMMAND ----------
# Final Validation

display(
    spark.sql(
        f"""
        SHOW TABLES IN {catalog_name}.gold
        """
    )
)

print("Fact Player Statistics:", spark.table(f"{catalog_name}.gold.fact_player_statistics").count())
print("Top Scorers:", spark.table(f"{catalog_name}.gold.top_scorers").count())
print("Top Assists:", spark.table(f"{catalog_name}.gold.top_assists").count())
print("Offensive Ranking:", spark.table(f"{catalog_name}.gold.player_offensive_ranking").count())
print("Defensive Discipline:", spark.table(f"{catalog_name}.gold.player_defensive_discipline").count())
print("Goalkeeper Ranking:", spark.table(f"{catalog_name}.gold.goalkeeper_ranking").count())
print("Team Performance:", spark.table(f"{catalog_name}.gold.team_performance").count())
print("Tournament Summary:", spark.table(f"{catalog_name}.gold.tournament_summary").count())
print("Match Details:", spark.table(f"{catalog_name}.gold.match_details").count())

print("Gold layer completed successfully.")
       