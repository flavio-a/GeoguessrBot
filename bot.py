import telegram, telegram.ext
import logging
import re
import urllib, urllib.request
import db_interface
import config

# Regexes definition
GEOGUESSR_RE = re.compile('(https://geoguessr.com/challenge/\w*)')
DATA_JSON_RE = re.compile('<script type="text/javascript">\s*window.apiModel =\s*(.*?);\s*</script>', flags=re.S)

# Useful global constants
updater = telegram.ext.Updater(token=config.TOKEN)
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
					level=logging.INFO)
db = db_interface.DBInterface(config.DB_NAME)

# Function handling '/start' command
def start(bot, update):
	bot.send_message(chat_id=update.message.chat_id, text="I'm a bot, please talk to me!")

# Function handling each message send in a group
def processMessage(bot, update):
	match = GEOGUESSR_RE.search(update.message.text)
	if match is not None:
		req = urllib.request.Request(match.group().replace('/challenge/', '/results/'))
		html_source = urllib.request.urlopen(req).read().decode("utf-8")
		json = DATA_JSON_RE.search(html_source).group(1)
		with open('pages/data.json', 'a') as f:
			f.write(json + '\n')
		bot.send_message(chat_id=update.message.chat_id, text='Trovato link di GeoGuessr')

# Binding handlers
updater.dispatcher.add_handler(telegram.ext.CommandHandler('start', start))
updater.dispatcher.add_handler(telegram.ext.MessageHandler(
		telegram.ext.Filters.text,
		processMessage
	))

# Start polling
updater.start_polling()
