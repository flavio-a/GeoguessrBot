import telegram, telegram.ext
import logging
import re
import urllib, urllib.request
import json

import db_interface
import config

# Regexes definition
GEOGUESSR_URL = 'https://geoguessr.com/challenge/'
GEOGUESSR_RE = re.compile(GEOGUESSR_URL + '(\w*)')
DATA_JSON_RE = re.compile('<script type="text/javascript">\s*window.apiModel =\s*(.*?);\s*</script>', flags=re.S)

# Useful global constants
updater = telegram.ext.Updater(token=config.TOKEN)
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
					level=logging.INFO)
db = db_interface.DBInterface(config.DB_NAME)

# Handler for '/start' command
def start(bot, update):
	bot.send_message(
		chat_id = update.message.chat_id,
		text = "Bot started! Type `/help` for more info"
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


# Given an URL, refresh the corresponding match in the DB, possibly creating it
def refreshMatch(fullurl, link):
	req = urllib.request.Request(fullurl)
	html_source = urllib.request.urlopen(req).read().decode("utf-8")
	match_data = DATA_JSON_RE.search(html_source).group(1)
	# with open('pages/data.json', 'a') as f:
	# 	f.write(match_data + '\n')
	db.updateMatch(link, json.loads(match_data))

# Handler for '/refresh' command
def refresh(bot, update, args):
	if args[0].lower() == 'all':
		links = db.getLinksList()
	else:
		links = [GEOGUESSR_RE.search(args[0]).group(1)]
	base_url = GEOGUESSR_URL.replace('/challenge/', '/results/')
	for link in links:
		refreshMatch(base_url + link, link)
		bot.send_message(chat_id=update.message.chat_id, text='Refreshed ' + base_url + link)


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

# Returns a list of couples (player, points) sorted by points, containing the
# total point for each people in the passed category
def calcPoints(mapType, timelimit):
	# print('calcPoints started')
	matches_list = db.getMatchesList(mapType, timelimit)
	total_scores = {}
	for match_id in matches_list:
		# print('Match ', match_id)
		scores = getPoints(db.getMatchResults(match_id))
		for p in scores:
			if p[0] in total_scores:
				total_scores[p[0]] += p[1]
			else:
				total_scores[p[0]] = p[1]
	result = []
	for player, points in total_scores.items():
		result.append((player, points))
	result.sort(reverse = True, key = lambda x: (x[1], x[0]))
	return result

# Handler for '/rank' command
def rank(bot, update, args):
	# print('Starting rank calc')
	mapType = args[0].lower() if len(args) > 0 else 'world'
	timelimit = args[1] if len(args) > 1 else 60
	text = "Rank up to date for category " + mapType + ", " + str(timelimit) + "s:\n* "
	points = calcPoints(mapType, timelimit)
	# print(points)
	text += "\n* ".join(map(
		lambda t: "{0:.2f}, ".format(t[1]) + t[0],
		points
	))
	bot.send_message(
		chat_id = update.message.chat_id,
		text = text
	)


# Handler for text messages processing
def processMessage(bot, update):
	match = GEOGUESSR_RE.search(update.message.text)
	if match is not None:
		refreshMatch(
			match.group(0).replace('/challenge/', '/results/'),
			match.group(1)
		)
		bot.send_message(chat_id=update.message.chat_id, text='Trovato link di GeoGuessr')


# Binding handlers
updater.dispatcher.add_handler(telegram.ext.CommandHandler('start', start))
updater.dispatcher.add_handler(telegram.ext.CommandHandler('rank', rank, pass_args=True))
updater.dispatcher.add_handler(telegram.ext.CommandHandler('leaderboards', leaderboards, pass_args=True))
updater.dispatcher.add_handler(telegram.ext.CommandHandler('refresh', refresh, pass_args=True))
updater.dispatcher.add_handler(telegram.ext.CommandHandler('whitelist', whitelist, pass_args=True))
updater.dispatcher.add_handler(telegram.ext.MessageHandler(
		telegram.ext.Filters.text,
		processMessage
	))

# Start polling
updater.start_polling()
