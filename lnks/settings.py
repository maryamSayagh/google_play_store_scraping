# Scrapy settings for lnks project
#
# For simplicity, this file contains only settings considered important or
# commonly used. You can find more settings consulting the documentation:
#
#     https://docs.scrapy.org/en/latest/topics/settings.html
#     https://docs.scrapy.org/en/latest/topics/downloader-middleware.html
#     https://docs.scrapy.org/en/latest/topics/spider-middleware.html

BOT_NAME = 'lnks'

SPIDER_MODULES = ['lnks.spiders']
NEWSPIDER_MODULE = 'lnks.spiders'

FEEDS = {
    'links/%(name)s/%(name)s_batch_%(batch_id)d.csv': {
        'format': 'csv',
        'batch_item_count': 10,
        }
}
# Crawl responsibly by identifying yourself (and your website) on the user-agent
#USER_AGENT = 'lnks (+http://www.yourdomain.com)'

# Obey robots.txt rules
#ROBOTSTXT_OBEY = True
ROBOTSTXT_OBEY = False

DOWNLOAD_DELAY = 0
DOWNLOAD_TIMEOUT = 30
RANDOMIZE_DOWNLOAD_DELAY = True

REACTOR_THREADPOOL_MAXSIZE = 128
CONCURRENT_REQUESTS = 256
CONCURRENT_REQUESTS_PER_DOMAIN = 256
CONCURRENT_REQUESTS_PER_IP = 256

AUTOTHROTTLE_ENABLED = True
AUTOTHROTTLE_START_DELAY = 1
AUTOTHROTTLE_MAX_DELAY = 0.25
AUTOTHROTTLE_TARGET_CONCURRENCY = 128
AUTOTHROTTLE_DEBUG = True

RETRY_ENABLED = True
RETRY_TIMES = 3
RETRY_HTTP_CODES = [500, 502, 503, 504, 400, 401, 403, 404, 405, 406, 407, 408, 409, 410, 429]

DOWNLOADER_MIDDLEWARES = {
    'scrapy.downloadermiddlewares.useragent.UserAgentMiddleware': None,
    'scrapy.spidermiddlewares.referer.RefererMiddleware': 80,
    'scrapy.downloadermiddlewares.retry.RetryMiddleware': 90,
    'scrapy_fake_useragent.middleware.RandomUserAgentMiddleware': 120,
    'scrapy.downloadermiddlewares.cookies.CookiesMiddleware': 130,
    'scrapy.downloadermiddlewares.httpcompression.HttpCompressionMiddleware': 810,
    'scrapy.downloadermiddlewares.redirect.RedirectMiddleware': 900,
    'lnks.middlewares.LnksDownloaderMiddleware': 1000
}
# Configure maximum concurrent requests performed by Scrapy (default: 16)
#CONCURRENT_REQUESTS = 32

# Configure a delay for requests for the same website (default: 0)
# See https://docs.scrapy.org/en/latest/topics/settings.html#download-delay
# See also autothrottle settings and docs
#DOWNLOAD_DELAY = 3
# The download delay setting will honor only one of:
#CONCURRENT_REQUESTS_PER_DOMAIN = 16
#CONCURRENT_REQUESTS_PER_IP = 16

# Disable cookies (enabled by default)
#COOKIES_ENABLED = False

# Disable Telnet Console (enabled by default)
#TELNETCONSOLE_ENABLED = False

# Override the default request headers:
#DEFAULT_REQUEST_HEADERS = {
#   'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
#   'Accept-Language': 'en',
#}

# Enable or disable spider middlewares
# See https://docs.scrapy.org/en/latest/topics/spider-middleware.html
#SPIDER_MIDDLEWARES = {
#    'lnks.middlewares.LnksSpiderMiddleware': 543,
#}

# Enable or disable downloader middlewares
# See https://docs.scrapy.org/en/latest/topics/downloader-middleware.html
#DOWNLOADER_MIDDLEWARES = {
#    'lnks.middlewares.LnksDownloaderMiddleware': 543,
#}

# Enable or disable extensions
# See https://docs.scrapy.org/en/latest/topics/extensions.html
#EXTENSIONS = {
#    'scrapy.extensions.telnet.TelnetConsole': None,
#}

# Configure item pipelines
# See https://docs.scrapy.org/en/latest/topics/item-pipeline.html
ITEM_PIPELINES = {
    'lnks.pipelines.LnksPipeline': 300,
    #'lnks.pipelines.JsonWriterPipeline': 800,
}

# Enable and configure the AutoThrottle extension (disabled by default)
# See https://docs.scrapy.org/en/latest/topics/autothrottle.html
#AUTOTHROTTLE_ENABLED = True
# The initial download delay
#AUTOTHROTTLE_START_DELAY = 5
# The maximum download delay to be set in case of high latencies
#AUTOTHROTTLE_MAX_DELAY = 60
# The average number of requests Scrapy should be sending in parallel to
# each remote server
#AUTOTHROTTLE_TARGET_CONCURRENCY = 1.0
# Enable showing throttling stats for every response received:
#AUTOTHROTTLE_DEBUG = False

# Enable and configure HTTP caching (disabled by default)
# See https://docs.scrapy.org/en/latest/topics/downloader-middleware.html#httpcache-middleware-settings
#HTTPCACHE_ENABLED = True
#HTTPCACHE_EXPIRATION_SECS = 0
#HTTPCACHE_DIR = 'httpcache'
#HTTPCACHE_IGNORE_HTTP_CODES = []
#HTTPCACHE_STORAGE = 'scrapy.extensions.httpcache.FilesystemCacheStorage'
SCRAPEOPS_API_KEY = ''
EXTENSIONS = {
    'scrapeops_scrapy.extension.ScrapeOpsMonitor': 500, 
}
DOWNLOADER_MIDDLEWARES = {
    'scrapeops_scrapy.middleware.retry.RetryMiddleware': 550,
    'scrapy.downloadermiddlewares.retry.RetryMiddleware': None,
}
