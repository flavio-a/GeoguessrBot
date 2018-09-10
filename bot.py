import telegram, telegram.ext
import logging
import re
import urllib, urllib.request
import db_interface
import config

GEOGUESSR_RE = re.compile('(https://geoguessr.com/challenge/\w*)')
DATA_JSON_RE = re.compile('<script type="text/javascript">\s*window.apiModel =\s*(.*?);\s*</script>', flags=re.S)

updater = telegram.ext.Updater(token=config.TOKEN)
dispatcher = updater.dispatcher
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
					level=logging.INFO)
db = db_interface.DBInterface(config.DB_NAME)

def start(bot, update):
	bot.send_message(chat_id=update.message.chat_id, text="I'm a bot, please talk to me!")

def processMessage(bot, update):
	match = GEOGUESSR_RE.search(update.message.text)
	if match is not None:
		# print(update.message.text)
		req = urllib.request.Request(match.group().replace('/challenge/', '/results/'))
		# req.add_header('Cookie', 'cookiename=cookievalue')
		html_source = urllib.request.urlopen(req).read().decode("utf-8")
		json = DATA_JSON_RE.search(html_source).group(1)
		with open('pages/data.json', 'a') as f:
			f.write(json + '\n')
		# subprocess.run(['wget', '--load-cookies', 'pages/cookies.txt', update.message.text])
		bot.send_message(chat_id=update.message.chat_id, text='Trovato link di GeoGuessr')

dispatcher.add_handler(telegram.ext.CommandHandler('start', start))
dispatcher.add_handler(telegram.ext.MessageHandler(
		telegram.ext.Filters.text,
		processMessage
	))

updater.start_polling()
