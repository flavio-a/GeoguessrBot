import sqlite3

# Interface to the DB
class DBInterface:
	def __init__(self, db_name):
		self.db_name = db_name
		self.db = sqlite3.connect(db_name)
		self.createdb()

	def __del__(self):
		self.db.close()

	# Creates the DB if it doesn't exist
	def createdb(self):
		cursor = self.db.cursor()
		cursor.execute('''
			CREATE TABLE IF NOT EXISTS players (
				id_player INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
				name VARCHAR(30),
				surname VARCHAR(30),
				UNIQUE(name, surname)
			)
		''')
		cursor.execute('''
			CREATE TABLE IF NOT EXISTS matches (
				id_match INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
				link VARCHAR(32) UNIQUE,
				map VARCHAR(50),
				timelimit INTEGER UNSIGNED
			)
		''')
		cursor.execute('''
			CREATE TABLE IF NOT EXISTS playersMatches (
				id_playerMatches INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
				id_player INTEGER,
				id_match INTEGER,
				total_score INTEGER,
				FOREIGN KEY (id_player) REFERENCES players(id_player)
				FOREIGN KEY (id_match) REFERENCES matches(id_match)
			)
		''')
		self.db.commit()

	# Add a player to the DB
	def createPlayer(self, name, surname):
		cursor = self.db.cursor()
		cursor.execute('''
			INSERT INTO players (name, surname)
			VALUES ('{name}', '{surname}');
		'''.format(name = name, surname = surname))
		self.db.commit()

	# Get the ID of a player from name and surname
	def getPlayerId(self, name, surname):
		cursor = self.db.cursor()
		query = '''
			SELECT id_player
			FROM players
			WHERE name = '{name}' AND surname = '{surname}'
		'''.format(name = name, surname = surname)
		fetches = cursor.execute(query).fetchall()
		if len(fetches) == 0:
			return None
		if len(fetches) > 1:
			# Should never happen because of uniqueness constraint, is here
			# just to be sure
			raise IndexError('Too many matches found with the passed link')
		return fetches[0][0]

	# Given name and surname, returns the player's ID. If they don't exist,
	# create them
	def findOrCreatePlayer(self, name, surname):
		cursor = self.db.cursor()
		query = '''
			SELECT id_player
			FROM players
			WHERE name = '{name}' AND surname = '{surname}'
		'''.format(name = name, surname = surname)
		fetches = cursor.execute(query).fetchall()
		if len(fetches) == 0:
			return None
		if len(fetches) > 1:
			# Should never happen because of uniqueness constraint, is here
			# just to be sure
			raise IndexError('Too many matches found with the passed link')
		return fetches[0][0]


	def createMatch(self, mapType, timelimit, link):
		cursor = self.db.cursor()
		cursor.execute('''
			INSERT INTO matches (map, timelimit)
			VALUES ('{map}', {timelimit});
		'''.format(map = mapType, timelimit = timelimit))
		self.db.commit()

	def getMatchId(self, link, mapType, timelimit):
		cursor = self.db.cursor()
		query = '''
			SELECT id_match
			FROM matches
			WHERE link = '{link}'
		'''.format(link = link)
		if mapType:
			query += "AND map = '{}'".format(mapType)
		if timelimit:
			query += 'AND timelimit = {}'.format(timelimit)
		fetches = cursor.execute(query).fetchall()
		if len(fetches) == 0:
			return None
		if len(fetches) > 1:
			# Should never happen because link is unique, is here just to be sure
			raise IndexError('Too many matches found with the passed link')
		return fetches[0][0]

	def createPlayerMatch(self):
		cursor = self.db.cursor()
		cursor.execute('''
			INSERT INTO playersMatches (id_player, id_match, total_score)
			VALUES ({id_player}, {id_match}, {total_score});
		'''.format(id_player = id_player, id_match = id_match, total_score = total_score))
		self.db.commit()

	def addMatch(self, **kwargs):
		self.createMatch(kwargs['link'], kwargs['mapSlug'], kwargs['roundTimeLimit'])
		match_id = self.getMatchId(kwargs['link'], kwargs['mapSlug'], kwargs['roundTimeLimit'])
		for player in kwargs['hiScores']:
			'''{
				"gameToken":"OLhmfKZ7ejQ1dihx",
				"playerName":"Manuele Cusumano",
				"totalScore":14156,
				"isActive":true,
				"isLeader":true,
				"pinUrl":"/Static/img/account/default-avatar.png",
				"color":"blue"
			}'''
