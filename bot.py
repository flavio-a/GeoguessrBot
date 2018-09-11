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

# Handler for '/rank' command
def rank(bot, update, args):
	mapType = args[0].lower() if len(args) > 0 else 'world'
	timelimit = args[1] if len(args) > 1 else 60
	text = "Rank up to date for category " + mapType + ", " + str(timelimit) + "s:\n* "
	scores = db.getScoreList(mapType, timelimit)
	text += "\n* ".join(map(lambda t: ", ".join(map(str, t)), scores))
	bot.send_message(
		chat_id = update.message.chat_id,
		text = text
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
def refreshMatch(url):
	req = urllib.request.Request(url)
	html_source = urllib.request.urlopen(req).read().decode("utf-8")
	match_data = DATA_JSON_RE.search(html_source).group(1)
	# with open('pages/data.json', 'a') as f:
	# 	f.write(match_data + '\n')
	db.updateMatch(GEOGUESSR_RE.search(url).group(1), json.loads(match_data))

# Handler for '/refresh' command
def refresh(bot, update, args):
	if args[0].lower() == 'all':
		links = db.getLinksList()
	else:
		links = [GEOGUESSR_RE.search(args[0]).group(1)]
	base_url = GEOGUESSR_URL.replace('/challenge/', '/results/')
	for link in links:
		refreshMatch(base_url + link)
		bot.send_message(chat_id=update.message.chat_id, text='Refreshed ' + base_url + link)

# Handler for text messages processing
def processMessage(bot, update):
	match = GEOGUESSR_RE.search(update.message.text)
	if match is not None:
		refreshMatch(match.group(0).replace('/challenge/', '/results/'))
		bot.send_message(chat_id=update.message.chat_id, text='Trovato link di GeoGuessr')


# Binding handlers
updater.dispatcher.add_handler(telegram.ext.CommandHandler('start', start))
updater.dispatcher.add_handler(telegram.ext.CommandHandler('rank', rank, pass_args=True))
updater.dispatcher.add_handler(telegram.ext.CommandHandler('leaderboards', leaderboards, pass_args=True))
updater.dispatcher.add_handler(telegram.ext.CommandHandler('refresh', refresh, pass_args=True))
updater.dispatcher.add_handler(telegram.ext.MessageHandler(
		telegram.ext.Filters.text,
		processMessage
	))

# Start polling
updater.start_polling()
