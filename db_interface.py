import psycopg2
import sqlalchemy
from sqlalchemy import Column
import datetime

Base = sqlalchemy.ext.declarative.declarative_base()

# Player class (mapped to db table players)
class Player(Base):
	__tablename__ = 'players'

	id = Column(sqlalchemy.Integer, primary_key = True, nullable = False)
	name = Column(sqlalchemy.String(60), unique = True)

# Matches class (mapped t db table matches)
class Match(Base):
	__tablename__ = 'matches'

	id = Column(sqlalchemy.Integer, primary_key = True, nullable = False)
	link = Column(sqlalchemy.String(32), unique = True)
	map = Column(sqlalchemy.String(50))
	timelimit = Column(sqlalchemy.Integer, sqlalchemy.CheckConstraint('timelimit > 0'))
	addtime = Column(sqlalchemy.DateTime, default = datetime.datetime.utcnow)

# PlayerMatches class (mapped to db table playerMatches)
class PlayerMatch(Base):
	__tablename__ = 'playerMatches'

	id = Column(sqlalchemy.Integer, primary_key = True, nullable = False)
	id_player = Column(
		sqlalchemy.Integer,
		nullable = False,
		ForeignKey("players.id")
	)
	id_match = Column(
		sqlalchemy.Integer,
		nullable = False,
		ForeignKey("matches.id")
	)
	total_score = Column(sqlalchemy.Integer)

	__table_args__ = (
        UniqueConstraint('id_player', 'id_match')
    )


# Interface to the DB
class DBInterface:
	def __init__(self, db_info, whitelist = None):
		# self.db_info = db_info
		self.engine = sqlalchemy.create_engine(db_info)
		self.createdb()
		if type(whitelist) ==  type([]):
			self.whitelist = [x.lower() for x in whitelist]
		else:
			self.loadWithelist()

	# Creates the DB if it doesn't exist
	def createdb(self):
		Base.metadata.create_all(self.engine)
		# db = psycopg2.connect(self.db_info)
		# cursor = db.cursor()
		# cursor.execute('''
		# 	CREATE TABLE IF NOT EXISTS players (
		# 		id_player SERIAL NOT NULL PRIMARY KEY,
		# 		name VARCHAR(60) UNIQUE
		# 	)
		# ''')
		# cursor.execute('''
		# 	CREATE TABLE IF NOT EXISTS matches (
		# 		id_match SERIAL NOT NULL PRIMARY KEY,
		# 		link VARCHAR(32) UNIQUE,
		# 		map VARCHAR(50),
		# 		timelimit NUMERIC CHECK (timelimit > 0),
		# 		addtime TIMESTAMP
		# 	)
		# ''')
		# cursor.execute('''
		# 	CREATE TABLE IF NOT EXISTS playerMatches (
		# 		id_playerMatches serial NOT NULL PRIMARY KEY,
		# 		id_player INTEGER REFERENCES players(id_player),
		# 		id_match INTEGER REFERENCES matches(id_match),
		# 		total_score INTEGER,
		# 		UNIQUE(id_player, id_match)
		# 	)
		# ''')
		# db.commit()
		# db.close()


	# Adds a name to the whitelist
	def addToWhitelist(self, name):
		if name.lower() not in self.whitelist:
			self.whitelist.append(name.lower())
			self.createPlayer(name)

	# Loads whitelist from the DB (adding each name saved)
	def loadWithelist(self):
		with sqlalchemy.orm.sessionmaker(bind = self.engine) as session:
			self.whitelist = [x.name.lower() for x in session.query(Player)]


	# Adds a player to the DB, only if they're in withelist
	def createPlayer(self, name):
		if name.lower() not in self.whitelist:
			return
		with sqlalchemy.orm.sessionmaker(bind = self.engine) as session:
			session.add(Player(name = name.lower()))

	# Gets the ID of a player from its name
	def getPlayerId(self, name):
		with sqlalchemy.orm.sessionmaker(bind = self.engine) as session:
			fetches = session.query(Player).filter_by(name = name)
			if len(fetches) == 0:
				return None
			if len(fetches) > 1:
				# Should never happen because of uniqueness constraint, is here
				# just to be sure
				raise IndexError('Too many matches found with the passed name')
			return fetches.first().id

	# Given name and surname, returns the player's ID. If they don't exist,
	# creates them. If name isn't in whitelist, returns None
	def findOrCreatePlayer(self, name):
		if name.lower() not in self.whitelist:
			return None
		player_id = self.getPlayerId(name)
		if player_id:
			return player_id
		self.createPlayer(name)
		return self.getPlayerId(name)


	# Adds a match without a category (that is, map and timelimit)
	def addEmptyMatch(self, link):
		with sqlalchemy.orm.sessionmaker(bind = self.engine) as session:
			session.add(Match(link = link, addtime = datetime.datetime.utcnow()))

	# Gets the ID of a match from its link
	def getMatchId(self, link):
		with sqlalchemy.orm.sessionmaker(bind = self.engine) as session:
			fetches = session.query(Match).filter_by(link = link)
			if len(fetches) == 0:
				return None
			if len(fetches) > 1:
				# Should never happen because link is unique, is here just to be sure
				raise IndexError('Too many matches found with the passed link')
			return fetches.first().id

	# Given link, returns the match's ID. If it doesn't exist, creates it
	# without category
	def findOrCreateMatch(self, link):
		match_id = self.getMatchId(link)
		if match_id:
			return match_id
		self.addEmptyMatch(link)
		return self.getMatchId(link)


	# Adds a playerMatch to the DB
	def createPlayerMatch(self, id_player, id_match, total_score):
		with sqlalchemy.orm.sessionmaker(bind = self.engine) as session:
			session.add(PlayerMatch(
				id_player = id_player,
				id_match = id_match,
				total_score = total_score
			))

	# Gets the ID of a player from name and surname
	def getPlayerMatchId(self, id_player, id_match):
		with sqlalchemy.orm.sessionmaker(bind = self.engine) as session:
			fetches = session.query(PlayerMatch).filter_by(
				id_player = id_player,
				id_match = id_match
			)
			if len(fetches) == 0:
				return None
			if len(fetches) > 1:
				# Should never happen because link is unique, is here just to be sure
				raise IndexError('Too many matches found with the passed link')
			return fetches.first().id

	# Given name and surname, returns the player's ID. If they don't exist,
	# creates them
	def findOrCreatePlayerMatch(self, id_player, id_match, total_score):
		playerMatch_id = self.getPlayerMatchId(id_player, id_match)
		if playerMatch_id:
			return playerMatch_id
		self.createPlayerMatch(id_player, id_match, total_score)
		return self.getPlayerMatchId(id_player, id_match)


	# Gets the list of saved links. If a datetime is passed, returns only
	# matches more recent than that date
	def getLinksList(self, since = None):
		with sqlalchemy.orm.sessionmaker(bind = self.engine) as session:
			fetches = session.query(Match.link)
			if since is not None:
				fetches = fetches.filter(Match.addtime >= since)
			return fetches

	# Gets the list of saved id_match for the passed category. If a datetime is
	# passed, returns only matches more recent than that date
	def getMatchesList(self, mapType, timelimit, since = None):
		with sqlalchemy.orm.sessionmaker(bind = self.engine) as session:
			fetches = session.query(Match.id).filter_by(
				map = mapType,
				timelimit = timelimit
			)
			if since is not None:
				fetches = fetches.filter(Match.addtime >= since)
			return fetches

	# Gets the list of links of matches that the player 'name' hasn't played
	def getUnplayedMatchesList(self, name):
		with sqlalchemy.orm.sessionmaker(bind = self.engine) as session:
			subquery = session.query(Match.id)
						.filter(Match.id == PlayerMatch.id_match)
						.filter(PlayerMatch.id_player == Player.id)
						.filter(Player.name == name)
			fetches = session.query(Match.link).filter(~Match.id.in_(subquery))
			return fetches


	# Updates a match in the DB, possibly creating any row needed. The first
	# parameter is the match's link, the second is the json from the page
	# parsed into a Python dict
	def updateMatch(self, link, json):
		with sqlalchemy.orm.sessionmaker(bind = self.engine) as session:
			this_match = session.query(Match).filter_by(link = link).first()
			id_match = this_match.id
			this_match.map = json['mapSlug']
			this_match.link = json['roundTimeLimit']
		# Possibly creates playermatch for each player
		for player in json['hiScores']:
			id_player = self.findOrCreatePlayer(player['playerName'])
			if id_player is not None:
				self.findOrCreatePlayerMatch(
					id_player,
					id_match,
					player['totalScore']
				)


	# Gets the list of pairs (player, score) for a single match from its ID
	def getMatchResults(self, match_id):
		with psycopg2.connect(self.db_info) as db:
			cursor = db.cursor()
			query = '''
				SELECT p.name, SUM(pm.total_score) AS score
				FROM playerMatches pm, players p
				WHERE p.id_player = pm.id_player
					AND pm.id_match = %(match_id)s
				GROUP BY p.id_player, p.name
				ORDER BY score DESC;
			'''
			cursor.execute(query, { 'match_id': match_id })
			return cursor.fetchall()

	# Gets the list of pairs (player, total_score) for the passed category
	def getScoreList(self, mapType, timelimit):
		with psycopg2.connect(self.db_info) as db:
			cursor = db.cursor()
			query = '''
				SELECT p.name, SUM(pm.total_score) AS score
				FROM playerMatches pm, players p, matches m
				WHERE p.id_player = pm.id_player
					AND pm.id_match = m.id_match
					AND m.map = %(map)s
					AND m.timelimit = %(timelimit)s
				GROUP BY p.id_player, p.name
				ORDER BY score DESC;
			'''
			cursor.execute(query, { 'map': mapType, 'timelimit': timelimit })
			return cursor.fetchall()

	# Gets a list of at most 3 pairs (player, total_score) containing the
	# records for the passed category
	def getLeaderbords(self, mapType, timelimit):
		with psycopg2.connect(self.db_info) as db:
			cursor = db.cursor()
			query = '''
				SELECT p.name, MAX(pm.total_score) AS score
				FROM playerMatches pm, players p, matches m
				WHERE p.id_player = pm.id_player
					AND pm.id_match = m.id_match
					AND m.map = %(map)s
					AND m.timelimit = %(timelimit)s
				GROUP BY p.id_player, p.name
				ORDER BY score DESC
				LIMIT 3;
			'''
			cursor.execute(query, { 'map': mapType, 'timelimit': timelimit })
			return cursor.fetchall()
