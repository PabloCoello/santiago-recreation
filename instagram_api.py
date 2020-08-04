from instagram.client import InstagramAPI

api = InstagramAPI(client_id='pablocoellopulido')

recent_media, next_ = api.tag_search('caminodesantiago', 10)
photos = []
for media in recent_media:
    photos.append('<img src="%s"/>' % media.images['thumbnail'].url)