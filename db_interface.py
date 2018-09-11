import sqlite3

# Interface to the DB
class DBInterface:
	def __init__(self, db_name, whitelist = None):
		self.db_name = db_name
		if type(whitelist) ==  type([]):
			self.whitelist = [x.lower() for x in whitelist]
		else:
			self.loadWithelist()
		self.createdb()

	# Creates the DB if it doesn't exist
	def createdb(self):
		db = sqlite3.connect(self.db_name)
		cursor = db.cursor()
		cursor.execute('''
			CREATE TABLE IF NOT EXISTS players (
				id_player INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
				name VARCHAR(60),
				UNIQUE(name)
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
				FOREIGN KEY (id_match) REFERENCES matches(id_match),
				UNIQUE(id_player, id_match)
			)
		''')
		db.commit()
		db.close()


	# Adds a name to the whitelist
	def addToWhitelist(self, name):
		if name.lower() not in self.whitelist:
			self.whitelist.append(name.lower())
			self.createPlayer(name)

	# Loads whitelist from the DB (adding each name saved)
	def loadWithelist(self):
		with sqlite3.connect(self.db_name) as db:
			cursor = db.cursor()
			query = '''
				SELECT p.name
				FROM players p;
			'''
			self.whitelist = [x[0].lower() for x in cursor.execute(query).fetchall()]


	# Adds a player to the DB, only if they're in withelist
	def createPlayer(self, name):
		if name.lower() not in self.whitelist:
			return
		db = sqlite3.connect(self.db_name)
		cursor = db.cursor()
		cursor.execute('''
			INSERT INTO players (name)
			VALUES ('{name}');
		'''.format(name = name))
		db.commit()
		db.close()

	# Gets the ID of a player from name and surname
	def getPlayerId(self, name):
		with sqlite3.connect(self.db_name) as db:
			cursor = db.cursor()
			query = '''
				SELECT id_player
				FROM players
				WHERE name = '{name}'
			'''.format(name = name)
			fetches = cursor.execute(query).fetchall()
			if len(fetches) == 0:
				return None
			if len(fetches) > 1:
				# Should never happen because of uniqueness constraint, is here
				# just to be sure
				raise IndexError('Too many matches found with the passed name')
			return fetches[0][0]

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


	# Adds a match to the DB
	def createMatch(self, link, mapType, timelimit):
		db = sqlite3.connect(self.db_name)
		cursor = db.cursor()
		cursor.execute('''
			INSERT INTO matches (link, map, timelimit)
			VALUES ('{link}', '{map}', {timelimit});
		'''.format(link = link, map = mapType, timelimit = timelimit))
		db.commit()
		db.close()

	# Gets the ID of a match from its link. Type of map and time limit are
	# optional
	def getMatchId(self, link, mapType, timelimit):
		with sqlite3.connect(self.db_name) as db:
			cursor = db.cursor()
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

	# Given link and possibly mapType and timelimit, returns the match's ID. If
	# it doesn't exist, creates it
	def findOrCreateMatch(self, link, mapType, timelimit):
		match_id = self.getMatchId(link, mapType, timelimit)
		if match_id:
			return match_id
		self.createMatch(link, mapType, timelimit)
		return self.getMatchId(link, mapType, timelimit)


	# Adds a playerMatch to the DB
	def createPlayerMatch(self, id_player, id_match, total_score):
		db = sqlite3.connect(self.db_name)
		cursor = db.cursor()
		cursor.execute('''
			INSERT INTO playersMatches (id_player, id_match, total_score)
			VALUES ({id_player}, {id_match}, {total_score});
		'''.format(
			id_player = id_player,
			id_match = id_match,
			total_score = total_score
		))
		db.commit()
		db.close()

	# Gets the ID of a player from name and surname
	def getPlayerMatchId(self, id_player, id_match, total_score):
		with sqlite3.connect(self.db_name) as db:
			cursor = db.cursor()
			query = '''
				SELECT id_playerMatches
				FROM playersMatches
				WHERE id_player = {id_player} AND id_match = {id_match}
			'''.format(id_player = id_player, id_match = id_match)
			if total_score:
				query += "AND total_score = '{}'".format(total_score)
			fetches = cursor.execute(query).fetchall()
			if len(fetches) == 0:
				return None
			if len(fetches) > 1:
				# Should never happen because of uniqueness constraint, is here
				# just to be sure
				raise IndexError('Too many matches found with the passed ids')
			return fetches[0][0]

	# Given name and surname, returns the player's ID. If they don't exist,
	# creates them
	def findOrCreatePlayerMatch(self, id_player, id_match, total_score):
		playerMatch_id = self.getPlayerMatchId(id_player, id_match, total_score)
		if playerMatch_id:
			return playerMatch_id
		self.createPlayerMatch(id_player, id_match, total_score)
		return self.getPlayerMatchId(id_player, id_match, total_score)

	# Gets the list of saved links
	def getLinksList(self):
		with sqlite3.connect(self.db_name) as db:
			cursor = db.cursor()
			query = '''
				SELECT m.link
				FROM matches m;
			'''
			return cursor.execute(query).fetchall()


	# Updates a match in the DB, possibly creating any row needed. The first
	# parameter is the match's link, the second is the json from the page
	# parsed into a Python dict
	def updateMatch(self, link, json):
		id_match = self.findOrCreateMatch(
							link,
							json['mapSlug'],
							json['roundTimeLimit']
						)
		for player in json['hiScores']:
			id_player = self.findOrCreatePlayer(player['playerName'])
			if id_player is not None:
				self.findOrCreatePlayerMatch(
					id_player,
					id_match,
					player['totalScore']
				)


	# Gets the list of pairs (player, total_score) for the passed category
	def getScoreList(self, mapType, timelimit):
		with sqlite3.connect(self.db_name) as db:
			cursor = db.cursor()
			query = '''
				SELECT p.name, SUM(pm.total_score) AS score
				FROM playersMatches pm, players p, matches m
				WHERE p.id_player = pm.id_player
					AND pm.id_match = m.id_match
					AND m.map = '{map}'
					AND m.timelimit = {timelimit}
				GROUP BY p.id_player, p.name
				ORDER BY score DESC;
			'''.format(map = mapType, timelimit = timelimit)
			return cursor.execute(query).fetchall()

	# Gets a list of at most 3 pairs (player, total_score) containing the
	# records for the passed category
	def getLeaderbords(self, mapType, timelimit):
		with sqlite3.connect(self.db_name) as db:
			cursor = db.cursor()
			query = '''
				SELECT p.name, MAX(pm.total_score) AS score
				FROM playersMatches pm, players p, matches m
				WHERE p.id_player = pm.id_player
					AND pm.id_match = m.id_match
					AND m.map = '{map}'
					AND m.timelimit = {timelimit}
				GROUP BY p.id_player, p.name
				ORDER BY score DESC
				LIMIT 3;
			'''.format(map = mapType, timelimit = timelimit)
			return cursor.execute(query).fetchall()
