import telegram, telegram.ext
import logging
import re
import urllib, urllib.request
import json
import operator
import datetime

import db_interface
import config

# Regexes definition
GEOGUESSR_URL = 'https://geoguessr.com/'
GEOGUESSR_RE = re.compile('(?:https?://)(?:www\.)?geoguessr.com/challenge/(\w*)')
DATA_JSON_RE = re.compile('<script type="text/javascript">\s*window.apiModel =\s*(.*?);\s*</script>', flags=re.S)

# Useful global constants
updater = telegram.ext.Updater(token = config.TOKEN)
logging.basicConfig(format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
					level = config.LOG_LEVEL,
					filename = config.LOG_FILE)
db = db_interface.DBInterface(config.DB_LOGIN_INFO)

# Handler for '/start' command
def start(bot, update):
	bot.send_message(
		chat_id = update.message.chat_id,
		text = "Bot started! Type `/help` for more info"
	)


# Handler for '/help' command
def help(bot, update):
	with open('help', 'r') as help_file:
		bot.send_message(
			chat_id = update.message.chat_id,
			text = help_file.read()
		)

# Handler for '/leaderboards' command
def leaderboards(bot, update, args):
	mapType = args[0].lower() if len(args) > 0 else 'world'
	timelimit = args[1] if len(args) > 1 else 60
	text = "Records for category " + mapType + ", " + str(timelimit) + "s:\n* "
	scores = db.getLeaderbords(mapType, timelimit)
	text += "\n* ".join(map(lambda t: ", ".join(map(str, t)), scores))
	bot.send_message(
		chat_id = update.message.chat_id,
		text = text
	)


# Given a link, refresh the corresponding match in the DB, possibly creating it
def refreshMatch(link):
	fullurl = GEOGUESSR_URL + 'results/' + link
	logging.debug('Asked refresh for link ' + link)
	logging.debug('Full URL: ' + fullurl)
	req = urllib.request.Request(fullurl)
	try:
		html_source = urllib.request.urlopen(req).read().decode("utf-8")
	except urllib.error.HTTPError as e:
		logging.info(str(e) + ': adding match with out info')
		db.findOrCreateMatch(link)
	else:
		logging.debug('Match refreshed succesfully')
		match_data = DATA_JSON_RE.search(html_source).group(1)
		db.updateMatch(link, json.loads(match_data))

# Handler for '/refresh' command
def refresh(bot, update, args):
	if len(args) == 0:
		bot.send_message(
			chat_id=update.message.chat_id,
			text='Argument needed!'
		)
		return
	if args[0].lower() == 'all':
		logging.debug('REFRESH: getting all links')
		links = db.getLinksList(None)
	elif args[0].lower() == 'recent':
		logging.debug('REFRESH: getting only recent links')
		links = db.getLinksList(datetime.datetime.now() - datetime.timedelta(days=14))
	else:
		links = [GEOGUESSR_RE.search(args[0]).group(1)]
	for link in links:
		refreshMatch(link)
		bot.send_message(chat_id=update.message.chat_id, text='Refreshed ' + GEOGUESSR_URL + 'challenge/' + link)
	bot.send_message(chat_id=update.message.chat_id, text='Finished refreshing')


# Handler fot '/whitelist' command
def whitelist(bot, update, args):
	if len(args) > 0:
		db.addToWhitelist(' '.join(args))
		print('Added ' + ' '.join(args))
		bot.send_message(
			chat_id = update.message.chat_id,
			text = 'Added ' + ' '.join(args) + ' to whitelist'
		)
	else:
		bot.send_message(
			chat_id = update.message.chat_id,
			text = "Current whitelist:\n" + "\n".join(db.whitelist)
		)


# Calc points for a match given the list of (players, scores) sorted by score.
# Returns a list in format (player, points)
def getPoints(scores):
	points = []
	n = len(scores)
	max_score = scores[0][1]
	max_bonus = 0.5 - 1 / n
	for i in range(n):
		bonus = max_bonus / (i + 1) if i < 3 else 0
		points.append((scores[i][0], scores[i][1] / max_score + bonus))
	return points

# Returns a pair of list of tuples: [(player, points)], [(player, avgPoints)]
# sorted by points, containing the total point and the average points per match
# for each people in the passed category
def calcPoints(mapType, timelimit):
	matches_list = db.getMatchesList(mapType, timelimit)
	total_scores = {}
	for match_id in matches_list:
		scores = getPoints(db.getMatchResults(match_id))
		for p in scores:
			if p[0] in total_scores:
				total_scores[p[0]] = tuple(map(
					operator.add,
					total_scores[p[0]],
					(p[1], 1)
				))
			else:
				total_scores[p[0]] = (p[1], 1)
	result1 = []
	result2 = []
	for player, points in total_scores.items():
		result1.append((player, points[0]))
		result2.append((player, points[0] / points[1]))
	result1.sort(reverse = True, key = lambda x: (x[1], x[0]))
	result2.sort(reverse = True, key = lambda x: (x[1], x[0]))
	return result1, result2

# Handler for '/rank' command
def rank(bot, update, args):
	# print('Starting rank calc')
	mapType = args[0].lower() if len(args) > 0 else 'world'
	timelimit = args[1] if len(args) > 1 else 60
	text = "Rank up to date for category " + mapType + ", " + str(timelimit) + "s:\n"
	points, avgPoints = calcPoints(mapType, timelimit)
	text += 'Total points:\n* '
	text += "\n* ".join(map(
		lambda t: "{0:.2f}, ".format(t[1]) + t[0],
		points
	))
	text += '\n\nAverage points:\n* '
	text += "\n* ".join(map(
		lambda t: "{0:.2f}, ".format(t[1]) + t[0],
		avgPoints
	))
	bot.send_message(
		chat_id = update.message.chat_id,
		text = text
	)


# Handler for '/toplay' command
def toPlay(bot, update, args):
	if len(args) == 0:
		bot.send_message(
			chat_id=update.message.chat_id,
			text='You should give a player name'
		)
		return
	name = ' '.join(args).lower()
	text = "Matches player " + name + " haven't played yet:\n* "
	text += "\n* ".join(map(
		lambda t: GEOGUESSR_URL + 'challenge/' + t,
		db.getUnplayedMatchesList(name)
	))
	bot.send_message(
		chat_id = update.message.chat_id,
		text = text
	)


# Handler for text messages processing
def processMessage(bot, update):
	match = GEOGUESSR_RE.search(update.message.text)
	if match is not None:
		refreshMatch(match.group(1))
		bot.send_message(chat_id=update.message.chat_id, text='Trovato link di GeoGuessr')


if __name__ == "__main__":
	# Binding handlers
	updater.dispatcher.add_handler(telegram.ext.CommandHandler('start', start))
	updater.dispatcher.add_handler(telegram.ext.CommandHandler('help', help))
	updater.dispatcher.add_handler(telegram.ext.CommandHandler('rank', rank, pass_args=True))
	updater.dispatcher.add_handler(telegram.ext.CommandHandler('leaderboards', leaderboards, pass_args=True))
	updater.dispatcher.add_handler(telegram.ext.CommandHandler('refresh', refresh, pass_args=True))
	updater.dispatcher.add_handler(telegram.ext.CommandHandler('whitelist', whitelist, pass_args=True))
	updater.dispatcher.add_handler(telegram.ext.CommandHandler('toplay', toPlay, pass_args=True))
	updater.dispatcher.add_handler(telegram.ext.MessageHandler(
			telegram.ext.Filters.text,
			processMessage
		))

	# Start polling
	print('Start polling')
	logging.debug('Start polling')
	updater.start_polling()
