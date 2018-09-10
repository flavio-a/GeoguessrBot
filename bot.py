import telegram, telegram.ext
import logging
import re
import urllib, urllib.request
import json

import db_interface
import config

# Regexes definition
GEOGUESSR_RE = re.compile('https://geoguessr.com/challenge/(\w*)')
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
	text = "Rank to the date for category " + mapType + ", " + str(timelimit) + "s:\n* "
	scores = db.getScoreList(mapType, timelimit)
	text += "\n* ".join(map(lambda t: ", ".join(map(str, t)), scores))
	bot.send_message(
		chat_id = update.message.chat_id,
		text = text
	)

# Handler for text messages processing
def processMessage(bot, update):
	match = GEOGUESSR_RE.search(update.message.text)
	if match is not None:
		req = urllib.request.Request(match.group(0).replace('/challenge/', '/results/'))
		html_source = urllib.request.urlopen(req).read().decode("utf-8")
		match_data = DATA_JSON_RE.search(html_source).group(1)
		with open('pages/data.json', 'a') as f:
			f.write(match_data + '\n')
		db.updateMatch(match.group(1), json.loads(match_data))
		bot.send_message(chat_id=update.message.chat_id, text='Trovato link di GeoGuessr')

# Binding handlers
updater.dispatcher.add_handler(telegram.ext.CommandHandler('start', start))
updater.dispatcher.add_handler(telegram.ext.CommandHandler('rank', rank, pass_args=True))
updater.dispatcher.add_handler(telegram.ext.MessageHandler(
		telegram.ext.Filters.text,
		processMessage
	))

# Start polling
updater.start_polling()
