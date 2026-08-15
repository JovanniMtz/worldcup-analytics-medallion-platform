# Databricks notebook source
# Databricks notebook source
# ============================================================
# FIFA World Cup 2026 Analytics Platform
# Silver Layer
# BRONZE Delta Tables -> SILVER Standardized Delta Tables
# ============================================================

from pyspark.sql.functions import *

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

silver_path = (
    f"abfss://silver-{environment}@"
    f"{storage_account}.dfs.core.windows.net/"
)

print(f"Environment : {environment}")
print(f"Catalog     : {catalog_name}")
print(f"SILVER Path : {silver_path}")

# COMMAND ----------
# Read Bronze tables

df_standard = spark.table(
    f"{catalog_name}.bronze.player_standard_stats2026_worldcup"
)

df_shooting = spark.table(
    f"{catalog_name}.bronze.player_shooting_stats2026_worldcup"
)

df_playingtime = spark.table(
    f"{catalog_name}.bronze.player_playingtime_stats2026_worldcup"
)

df_misc = spark.table(
    f"{catalog_name}.bronze.player_miscellaneous_stats2026_worldcup"
)

df_goalkeeping = spark.table(
    f"{catalog_name}.bronze.player_goalkeeping_2026_worldcup"
)

df_matches = spark.table(
    f"{catalog_name}.bronze.scores_fixtures_2026_worldcup"
)

print("Bronze tables loaded successfully.")

print("Standard:", df_standard.count())
print("Shooting:", df_shooting.count())
print("Playing Time:", df_playingtime.count())
print("Miscellaneous:", df_misc.count())
print("Goalkeeping:", df_goalkeeping.count())
print("Scores Fixtures:", df_matches.count())

# COMMAND ----------
# Player Standard

df_standard_silver = (
    df_standard
        .withColumnRenamed("Rk", "Ranking")
        .withColumnRenamed("Pos", "PlayerPosition")
        .withColumnRenamed("Squad", "NationalTeam")
        .withColumnRenamed("Player", "PlayerName")
        .withColumnRenamed("Age", "PlayerAge")
        .withColumnRenamed("Born", "BirthYear")
        .withColumnRenamed("Club", "PlayerClub")
        .withColumnRenamed("MP", "MatchesPlayed")
        .withColumnRenamed("Starts", "MatchesStarted")
        .withColumnRenamed("Min", "MinutesPlayed")
        .withColumnRenamed("90s", "Equivalent90MinMatches")
        .withColumnRenamed("Gls11", "Goals")
        .withColumnRenamed("Ast12", "Assists")
        .withColumnRenamed("G+A13", "GoalsAssists")
        .withColumnRenamed("G-PK14", "GoalsWithoutPenalty")
        .withColumnRenamed("PK", "PenaltyGoals")
        .withColumnRenamed("PKatt", "PenaltyAttempts")
        .withColumnRenamed("CrdY", "YellowCards")
        .withColumnRenamed("CrdR", "RedCards")
        .withColumnRenamed("Gls19", "GoalsPer90")
        .withColumnRenamed("Ast20", "AssistsPer90")
        .withColumnRenamed("G+A21", "GoalsAssistsPer90")
        .withColumnRenamed("G-PK22", "GoalsWithoutPenaltyPer90")
        .withColumnRenamed("G+A-PK", "GoalsAssistsWithoutPenaltyPer90")
        .withColumnRenamed("Matches", "MatchLink")
        .withColumnRenamed("-9999", "PlayerId")
        .withColumn(
            "CreatedDate",
            current_timestamp()
        )
)

display(df_standard_silver.limit(10))

(
    df_standard_silver.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .option(
            "path",
            f"{silver_path}player_standard"
        )
        .saveAsTable(
            f"{catalog_name}.silver.player_standard"
        )
)

print("Silver table created: player_standard")

# COMMAND ----------
# Dim Player

dim_player = (
    df_standard_silver
        .select(
            "PlayerId",
            "PlayerName",
            "PlayerPosition",
            "NationalTeam",
            "PlayerClub",
            "PlayerAge",
            "BirthYear"
        )
        .dropDuplicates()
        .withColumn(
            "CreatedDate",
            current_timestamp()
        )
)

display(dim_player.limit(10))

(
    dim_player.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .option(
            "path",
            f"{silver_path}dim_player"
        )
        .saveAsTable(
            f"{catalog_name}.silver.dim_player"
        )
)

print("Silver table created: dim_player")

# COMMAND ----------
# Player Shooting

df_shooting_silver = (
    df_shooting
        .withColumnRenamed("Player", "PlayerName")
        .withColumnRenamed("Pos", "PlayerPosition")
        .withColumnRenamed("Squad", "NationalTeam")
        .withColumnRenamed("Age", "PlayerAge")
        .withColumnRenamed("Born", "BirthYear")
        .withColumnRenamed("-9999", "PlayerId")
        .withColumnRenamed("90s", "Equivalent90MinMatches")
        .withColumnRenamed("Gls", "Goals")
        .withColumnRenamed("PK", "PenaltyGoals")
        .withColumnRenamed("PKatt", "PenaltyAttempts")
        .withColumnRenamed("Sh", "TotalShots")
        .withColumnRenamed("SoT", "ShotsOnTarget")
        .withColumnRenamed("SoT%", "ShotsOnTargetPercentage")
        .withColumnRenamed("Sh/90", "ShotsPer90")
        .withColumnRenamed("SoT/90", "ShotsOnTargetPer90")
        .withColumnRenamed("G/Sh", "GoalsPerShot")
        .withColumnRenamed("G/SoT", "GoalsPerShotOnTarget")
        .withColumnRenamed("Matches", "MatchReference")
        .withColumn(
            "CreatedDate",
            current_timestamp()
        )
)

display(df_shooting_silver.limit(10))

(
    df_shooting_silver.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .option(
            "path",
            f"{silver_path}player_shooting"
        )
        .saveAsTable(
            f"{catalog_name}.silver.player_shooting"
        )
)

print("Silver table created: player_shooting")

# COMMAND ----------
# Player Playing Time

df_playingtime_silver = (
    df_playingtime
        .withColumnRenamed("Player", "PlayerName")
        .withColumnRenamed("Pos", "PlayerPosition")
        .withColumnRenamed("Squad", "NationalTeam")
        .withColumnRenamed("Age", "PlayerAge")
        .withColumnRenamed("Born", "BirthYear")
        .withColumnRenamed("-9999", "PlayerId")
        .withColumnRenamed("MP", "MatchesPlayed")
        .withColumnRenamed("Min", "MinutesPlayed")
        .withColumnRenamed("90s", "Equivalent90MinMatches")
        .withColumnRenamed("Starts", "MatchesStarted")
        .withColumnRenamed("Mn/MP", "AverageMinutesPerMatch")
        .withColumnRenamed("Min%", "TeamMinutesPercentage")
        .withColumnRenamed("Mn/Start", "AverageMinutesPerStart")
        .withColumnRenamed("Compl", "CompleteMatches")
        .withColumnRenamed("Subs", "SubstituteAppearances")
        .withColumnRenamed("Mn/Sub", "AverageMinutesAsSubstitute")
        .withColumnRenamed("unSub", "UnusedSubstituteMatches")
        .withColumnRenamed("PPM", "PointsPerMatch")
        .withColumnRenamed("onG", "GoalsForWhileOnField")
        .withColumnRenamed("onGA", "GoalsAgainstWhileOnField")
        .withColumnRenamed("+/-", "GoalDifference")
        .withColumnRenamed("+/-90", "GoalDifferencePer90")
        .withColumnRenamed("On-Off", "OnOffGoalDifference")
        .withColumnRenamed("Matches", "MatchReference")
        .withColumn(
            "CreatedDate",
            current_timestamp()
        )
)

display(df_playingtime_silver.limit(10))

(
    df_playingtime_silver.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .option(
            "path",
            f"{silver_path}player_playing_time"
        )
        .saveAsTable(
            f"{catalog_name}.silver.player_playing_time"
        )
)

print("Silver table created: player_playing_time")

# COMMAND ----------
# Player Miscellaneous

df_misc_silver = (
    df_misc
        .withColumnRenamed("Player", "PlayerName")
        .withColumnRenamed("Pos", "PlayerPosition")
        .withColumnRenamed("Squad", "NationalTeam")
        .withColumnRenamed("Age", "PlayerAge")
        .withColumnRenamed("Born", "BirthYear")
        .withColumnRenamed("-9999", "PlayerId")
        .withColumnRenamed("90s", "Equivalent90MinMatches")
        .withColumnRenamed("CrdY", "YellowCards")
        .withColumnRenamed("CrdR", "RedCards")
        .withColumnRenamed("2CrdY", "SecondYellowRedCards")
        .withColumnRenamed("Fls", "FoulsCommitted")
        .withColumnRenamed("Fld", "FoulsReceived")
        .withColumnRenamed("Off", "Offsides")
        .withColumnRenamed("Crs", "Crosses")
        .withColumnRenamed("Int", "Interceptions")
        .withColumnRenamed("TklW", "TacklesWon")
        .withColumnRenamed("PKwon", "PenaltiesWon")
        .withColumnRenamed("PKcon", "PenaltiesConceded")
        .withColumnRenamed("OG", "OwnGoals")
        .withColumnRenamed("Matches", "MatchReference")
        .withColumn(
            "CreatedDate",
            current_timestamp()
        )
)

display(df_misc_silver.limit(10))

(
    df_misc_silver.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .option(
            "path",
            f"{silver_path}player_miscellaneous"
        )
        .saveAsTable(
            f"{catalog_name}.silver.player_miscellaneous"
        )
)

print("Silver table created: player_miscellaneous")

# COMMAND ----------
# Player Goalkeeping

df_goalkeeping_silver = (
    df_goalkeeping
        .withColumnRenamed("Player", "PlayerName")
        .withColumnRenamed("Pos", "PlayerPosition")
        .withColumnRenamed("Squad", "NationalTeam")
        .withColumnRenamed("Age", "PlayerAge")
        .withColumnRenamed("Club", "PlayerClub")
        .withColumnRenamed("Born", "BirthYear")
        .withColumnRenamed("-9999", "PlayerId")
        .withColumnRenamed("MP", "MatchesPlayed")
        .withColumnRenamed("Starts", "MatchesStarted")
        .withColumnRenamed("Min", "MinutesPlayed")
        .withColumnRenamed("90s", "Equivalent90MinMatches")
        .withColumnRenamed("GA", "GoalsAgainst")
        .withColumnRenamed("GA90", "GoalsAgainstPer90")
        .withColumnRenamed("SoTA", "ShotsOnTargetAgainst")
        .withColumnRenamed("Saves", "Saves")
        .withColumnRenamed("Save%15", "SavePercentage")
        .withColumnRenamed("W", "Wins")
        .withColumnRenamed("D", "Draws")
        .withColumnRenamed("L", "Losses")
        .withColumnRenamed("CS", "CleanSheets")
        .withColumnRenamed("CS%", "CleanSheetPercentage")
        .withColumnRenamed("PKatt", "PenaltyAttemptsAgainst")
        .withColumnRenamed("PKA", "PenaltyGoalsAgainst")
        .withColumnRenamed("PKsv", "PenaltySaves")
        .withColumnRenamed("PKm", "PenaltyMissesAgainst")
        .withColumnRenamed("Save%25", "PenaltySavePercentage")
        .withColumnRenamed("Matches", "MatchReference")
        .withColumn(
            "CreatedDate",
            current_timestamp()
        )
)

display(df_goalkeeping_silver.limit(10))

(
    df_goalkeeping_silver.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .option(
            "path",
            f"{silver_path}player_goalkeeping"
        )
        .saveAsTable(
            f"{catalog_name}.silver.player_goalkeeping"
        )
)

print("Silver table created: player_goalkeeping")

# COMMAND ----------
# Match Fixtures

df_matches_silver = (
    df_matches
        .withColumnRenamed("Round", "TournamentRound")
        .withColumnRenamed("Wk", "TournamentWeek")
        .withColumnRenamed("Day", "MatchDay")
        .withColumnRenamed("Date", "MatchDate")
        .withColumnRenamed("Time", "MatchTime")
        .withColumnRenamed("Home", "HomeTeam")
        .withColumnRenamed("Away", "AwayTeam")
        .withColumnRenamed("Score", "FinalScore")
        .withColumnRenamed("Attendance", "Attendance")
        .withColumnRenamed("Venue", "Venue")
        .withColumnRenamed("Referee", "Referee")
        .withColumnRenamed("MatchReport", "MatchReportLink")
        .withColumnRenamed("Notes", "MatchNotes")
        .withColumn(
            "CreatedDate",
            current_timestamp()
        )
)

display(df_matches_silver.limit(10))

(
    df_matches_silver.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .option(
            "path",
            f"{silver_path}match_fixtures"
        )
        .saveAsTable(
            f"{catalog_name}.silver.match_fixtures"
        )
)

print("Silver table created: match_fixtures")

# COMMAND ----------
# Validation

display(
    spark.sql(
        f"""
        SHOW TABLES IN {catalog_name}.silver
        """
    )
)

print("Player Standard:", spark.table(f"{catalog_name}.silver.player_standard").count())
print("Dim Player:", spark.table(f"{catalog_name}.silver.dim_player").count())
print("Player Shooting:", spark.table(f"{catalog_name}.silver.player_shooting").count())
print("Player Playing Time:", spark.table(f"{catalog_name}.silver.player_playing_time").count())
print("Player Miscellaneous:", spark.table(f"{catalog_name}.silver.player_miscellaneous").count())
print("Player Goalkeeping:", spark.table(f"{catalog_name}.silver.player_goalkeeping").count())
print("Match Fixtures:", spark.table(f"{catalog_name}.silver.match_fixtures").count())

print("Silver layer completed successfully.")

# COMMAND ----------

