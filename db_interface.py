import psycopg2
import sqlalchemy, sqlalchemy.ext, sqlalchemy.ext.declarative
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

# PlayerMatches class (mapped to db table playermatches)
class PlayerMatch(Base):
	__tablename__ = 'playermatches'

	id = Column(sqlalchemy.Integer, primary_key = True, nullable = False)
	id_player = Column(
		sqlalchemy.Integer,
		sqlalchemy.ForeignKey("players.id"),
		nullable = False
	)
	id_match = Column(
		sqlalchemy.Integer,
		sqlalchemy.ForeignKey("matches.id"),
		nullable = False
	)
	total_score = Column(sqlalchemy.Integer)

	__table_args__ = (
        sqlalchemy.UniqueConstraint('id_player', 'id_match'),
    )

# Season class (mapped to db table seasons)
class Season(Base):
	__tablename__ = 'seasons'

	id = Column(sqlalchemy.Integer, primary_key = True, nullable = False)
	num = Column(
		sqlalchemy.Integer,
		sqlalchemy.Sequence('seasons_num_sequence', start = 1, increment = 1),
		unique = True,
		nullable = False
	)
	endtime = Column(sqlalchemy.DateTime)


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

	# Adds a name to the whitelist
	def addToWhitelist(self, name):
		if name.lower() not in self.whitelist:
			self.whitelist.append(name.lower())
			self.createPlayer(name)

	# Loads whitelist from the DB (adding each name saved)
	def loadWithelist(self):
		session = sqlalchemy.orm.sessionmaker(bind = self.engine)()
		self.whitelist = [x[0] for x in session.query(Player.name).all()]


	# Adds a player to the DB, only if they're in withelist
	def createPlayer(self, name, **kwargs):
		if name.lower() not in self.whitelist:
			return
		if 'session' in kwargs:
			session = kwargs['session']
		else:
			session = sqlalchemy.orm.sessionmaker(bind = self.engine)()
		session.add(Player(name = name.lower()))
		if 'session' not in kwargs:
			session.commit()

	# Gets the ID of a player from its name
	def getPlayerId(self, name, **kwargs):
		if 'session' in kwargs:
			session = kwargs['session']
		else:
			session = sqlalchemy.orm.sessionmaker(bind = self.engine)()
		# Should never find more than one because of uniqueness constraint
		pl = session.query(Player.id).filter_by(name = name.lower())\
				.one_or_none()
		return pl[0] if pl is not None else pl

	# Given name and surname, returns the player's ID. If they don't exist,
	# creates them. If name isn't in whitelist, returns None
	def findOrCreatePlayer(self, name, **kwargs):
		if name.lower() not in self.whitelist:
			return None
		session = kwargs['session'] if 'session' in kwargs else None
		player_id = self.getPlayerId(name, session = session)
		if player_id:
			return player_id
		self.createPlayer(name, session = session)
		return self.getPlayerId(name, session = session)


	# Adds a match without a category (that is, map and timelimit)
	def addEmptyMatch(self, link, **kwargs):
		if 'session' in kwargs:
			session = kwargs['session']
		else:
			session = sqlalchemy.orm.sessionmaker(bind = self.engine)()
		session.add(Match(link = link, addtime = datetime.datetime.utcnow()))
		if 'session' not in kwargs:
			session.commit()

	# Gets the ID of a match from its link
	def getMatchId(self, link, **kwargs):
		if 'session' in kwargs:
			session = kwargs['session']
		else:
			session = sqlalchemy.orm.sessionmaker(bind = self.engine)()
		# Should never find more than one because of uniqueness constraint
		mtc = session.query(Match.id).filter_by(link = link).one_or_none()
		return mtc[0] if mtc is not None else mtc

	# Given link, returns the match's ID. If it doesn't exist, creates it
	# without category
	def findOrCreateMatch(self, link, **kwargs):
		session = kwargs['session'] if 'session' in kwargs else None
		match_id = self.getMatchId(link, session = session)
		if match_id:
			return match_id
		self.addEmptyMatch(link, session = session)
		return self.getMatchId(link, session = session)


	# Adds a playerMatch to the DB
	def createPlayerMatch(self, id_player, id_match, total_score, **kwargs):
		if 'session' in kwargs:
			session = kwargs['session']
		else:
			session = sqlalchemy.orm.sessionmaker(bind = self.engine)()
		session.add(PlayerMatch(
			id_player = id_player,
			id_match = id_match,
			total_score = total_score
		))
		if 'session' not in kwargs:
			session.commit()

	# Gets the ID of a player from name and surname
	def getPlayerMatchId(self, id_player, id_match, **kwargs):
		if 'session' in kwargs:
			session = kwargs['session']
		else:
			session = sqlalchemy.orm.sessionmaker(bind = self.engine)()
		# Should never find more than one because of uniqueness constraint
		pm = session.query(PlayerMatch.id).filter_by(
			id_player = id_player,
			id_match = id_match
		).one_or_none()
		return pm[0] if pm is not None else pm

	# Given name and surname, returns the player's ID. If they don't exist,
	# creates them
	def findOrCreatePlayerMatch(self, id_player, id_match, total_score, **kwargs):
		session = kwargs['session'] if 'session' in kwargs else None
		playerMatch_id = self.getPlayerMatchId(
			id_player,
			id_match,
			session = session
		)
		if playerMatch_id:
			return playerMatch_id
		self.createPlayerMatch(
			id_player,
			id_match,
			total_score,
			session = session
		)
		return self.getPlayerMatchId(id_player, id_match, session = session)


	# Get the season of a match (from it's id)
	def getMatchSeason(self, id_match, **kwargs):
		if 'session' in kwargs:
			session = kwargs['session']
		else:
			session = sqlalchemy.orm.sessionmaker(bind = self.engine)()
		return session.query(sqlalchemy.func.min(Season.num))\
			.filter(sqlalchemy.or_(Season.endtime >= Match.addtime, Season.endtime == None))\
			.filter(Match.id == id_match).one()[0]

	# Get current season
	def getCurrentSeason(self):
		session = sqlalchemy.orm.sessionmaker(bind = self.engine)()
		return session.query(sqlalchemy.func.max(Season.num)).one()[0]

	# Decides whether a match should be updated or not
	def shouldUpdate(self, id_match, **kwargs):
		if 'session' in kwargs:
			session = kwargs['session']
		else:
			session = sqlalchemy.orm.sessionmaker(bind = self.engine)()
		season_num = self.getMatchSeason(id_match, session = session)
		season_end = session.query(Season.endtime)\
			.filter(Season.num == season_num).one()[0]
		if season_end is None:
			return True
		else:
			return season_end >= datetime.datetime.now() - datetime.timedelta(days = 1)


	# Gets the list of saved links. If a datetime is passed, returns only
	# matches more recent than that date
	def getLinksList(self, since = None):
		session = sqlalchemy.orm.sessionmaker(bind = self.engine)()
		fetches = session.query(Match.link)
		if since is not None:
			fetches = fetches.filter(Match.addtime >= since)
		return [x[0] for x in fetches.all()]

	# Gets the list of links in a certain season
	def getSeasonList(self, season = None):
		if season is None:
			season = self.getCurrentSeason()
		session = sqlalchemy.orm.sessionmaker(bind = self.engine)()
		m = sqlalchemy.orm.aliased(Match, name='m')
		subquery = session.query(sqlalchemy.func.min(Season.num))\
			.filter(sqlalchemy.or_(Season.endtime >= Match.addtime, Season.endtime == None))\
			.filter(Match.id == m.id)
		fetches = session.query(m.link)\
			.filter(season == sqlalchemy.all_(subquery))
		return [x[0] for x in fetches.all()]


	# Gets the list of saved id_match for the passed category. If a datetime is
	# passed, returns only matches more recent than that date
	def getMatchesList(self, mapType, timelimit, since = None):
		session = sqlalchemy.orm.sessionmaker(bind = self.engine)()
		fetches = session.query(Match.id).filter_by(
			map = mapType,
			timelimit = timelimit
		)
		if since is not None:
			fetches = fetches.filter(Match.addtime >= since)
		return [x[0] for x in fetches.all()]

	# Gets the list of links of matches that the player 'name' hasn't played
	def getUnplayedMatchesList(self, name):
		session = sqlalchemy.orm.sessionmaker(bind = self.engine)()
		subquery = session.query(Match.id)\
					.filter(Match.id == PlayerMatch.id_match)\
					.filter(PlayerMatch.id_player == Player.id)\
					.filter(Player.name == name)
		fetches = session.query(Match.link).filter(~Match.id.in_(subquery))
		return [x[0] for x in fetches.all()]


	# Updates a match in the DB, possibly creating any row needed. The first
	# parameter is the match's link, the second is the json from the page
	# parsed into a Python dict
	# A match in a closed season won't be updated
	def updateMatch(self, link, json):
		session = sqlalchemy.orm.sessionmaker(bind = self.engine)()
		this_match = session.query(Match).filter_by(link = link).first()
		id_match = this_match.id
		if not self.shouldUpdate(id_match, season = season):
			# closed season
			return
		this_match.map = json['mapSlug']
		this_match.timelimit = json['roundTimeLimit']
		# Possibly creates playermatch for each player
		for player in json['hiScores']:
			id_player = self.findOrCreatePlayer(
				player['playerName'],
				session = session
			)
			if id_player is not None:
				self.findOrCreatePlayerMatch(
					id_player,
					id_match,
					player['totalScore'],
					session = session
				)
		session.commit()


	# Gets the list of pairs (player, score) for a single match from its ID
	def getMatchResults(self, match_id):
		session = sqlalchemy.orm.sessionmaker(bind = self.engine)()
		return session\
			.query(Player.name, sqlalchemy.func.sum(PlayerMatch.total_score))\
			.filter(Player.id == PlayerMatch.id_player)\
			.filter(PlayerMatch.id_match == match_id)\
			.group_by(Player.id, Player.name)\
			.order_by(sqlalchemy.func.sum(PlayerMatch.total_score).desc())\
			.all()

	# Gets the list of pairs (player, total_score) for the passed category
	def getScoreList(self, mapType, timelimit):
		session = sqlalchemy.orm.sessionmaker(bind = self.engine)()
		return session\
			.query(Player.name, sqlalchemy.func.sum(PlayerMatch.total_score))\
			.filter(Player.id == PlayerMatch.id_player)\
			.filter(PlayerMatch.id_match == Match.id)\
			.filter(Match.map == mapType, Match.timelimit == timelimit)\
			.group_by(Player.id, Player.name)\
			.order_by(sqlalchemy.func.sum(PlayerMatch.total_score).desc())\
			.all()

	# Gets a list of at most 3 pairs (player, total_score) containing the
	# records for the passed category
	def getLeaderbords(self, mapType, timelimit):
		session = sqlalchemy.orm.sessionmaker(bind = self.engine)()
		return session\
			.query(Player.name, sqlalchemy.func.max(PlayerMatch.total_score))\
			.filter(Player.id == PlayerMatch.id_player)\
			.filter(PlayerMatch.id_match == Match.id)\
			.filter(Match.map == mapType, Match.timelimit == timelimit)\
			.group_by(Player.id, Player.name)\
			.order_by(sqlalchemy.func.max(PlayerMatch.total_score).desc())\
			.limit(3).all()
