import requests
import urllib

class Archief:
   def pid_to_url(pid, search_string = ''):
      '''Convert a pid to the url for easy verification
      '''
      try:
         pid = pid.split('_')[0]
         url = 'https://hetarchief.be/en/pid/' + pid
         res = requests.head(url)
         location = res.headers['location']
         url = location + '?' + urllib.parse.urlencode({"search": search_string})
      except Exception as e:
         print(e)
      return url
