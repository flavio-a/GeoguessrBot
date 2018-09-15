import bot

if __name__ == "__main__":
	print('Refreshing all...')
	links = bot.db.getLinksList()
	for link in links:
		bot.refreshMatch(link)
	print('Refreshed succesfully')
