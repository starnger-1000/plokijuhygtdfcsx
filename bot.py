# bot.py
# Full Club Auction Bot (MongoDB + Premium Embeds + Stability Patches)
# Dependencies: discord.py, fastapi, uvicorn, jinja2, pymongo, dnspython
# Install: pip install discord.py fastapi uvicorn jinja2 pymongo dnspython

import os
import asyncio
import random
from datetime import datetime
import discord
from discord.ext import commands
from pymongo import MongoClient, ReturnDocument

# ---------- CONFIG ----------
DISCORD_TOKEN = os.getenv("DISCORD_TOKEN")
MONGO_URL = os.getenv("MONGO_URL")

BOT_OWNER_ID = int(os.getenv("BOT_OWNER_ID")) if os.getenv("BOT_OWNER_ID") else None
REPORT_CHANNEL_ID = int(os.getenv("REPORT_CHANNEL_ID")) if os.getenv("REPORT_CHANNEL_ID") else None

# Auction config
TIME_LIMIT = 30
MIN_INCREMENT_PERCENT = 5
LEAVE_PENALTY_PERCENT = 10
DUELIST_MISS_PENALTY_PERCENT = 15

# Market/Level Config
WIN_VALUE_BONUS = 100000
LOSS_VALUE_PENALTY = -100000
OWNER_MSG_VALUE_BONUS = 10000
OWNER_MSG_COUNT_PER_BONUS = 100

LEVEL_UP_CONFIG = [
    (12, "5th Division", 50000),
    (27, "4th Division", 100000),
    (45, "3rd Division", 150000),
    (66, "2nd Division", 200000),
    (90, "1st Division", 300000),
    (117, "17th Position", 320000),
    (147, "15th Position", 360000),
    (180, "12th Position", 400000),
    (216, "10th Position", 450000),
    (255, "8th Position", 500000),
    (297, "6th Position", 550000),
    (342, "Conference League", 600000),
    (390, "5th Position", 650000),
    (441, "Europa League", 700000),
    (495, "4th Position", 750000),
    (552, "3rd Position", 800000),
    (612, "Champions League", 900000),
    (675, "2nd Position", 950000),
    (741, "1st Position and League Winner", 1000000),
    (810, "UCL Winner", 1500000),
    (882, "Treble Winner", 2000000),
]

# ---------- EMOJI CONFIG ----------
E_ACTIVE = "<a:geeen_dot:1417645972787298355>"
E_DANGER = "<a:red_dot:1417646052915548263>"
E_ALERT = "<a:alert:1417586766331777134>" 
E_ERROR = "<a:cross2:972155180185452544>"
E_SUCCESS = "<a:verified:962942818886770688>"
E_GOLD_TICK = "<a:goldcheckmark:1430424224039964683>"
E_CROWN = "<a:crownop:962190451744579605>"
E_ADMIN = "<a:HeadAdmin_red:1423124795507085343>"
E_PREMIUM = "<a:donate_red:1424153582462177291>"
E_BOOST = "<a:boost:962277213204525086>"
E_PIKACHU = "<a:miapikachu:1430351344451063839>"
E_MONEY = "<a:Donation:962944611792326697>"
E_TIMER = "<a:1031pixelclock:1435781244452602029>"
E_STARS = "<a:bluestars:1418044149852278914>"
E_BOX = "<a:itembox:1418044218789728397>"
E_FIRE = "<a:redfire1:1417227382779281592>"
E_ARROW = "<a:arrow_arrow:962945777821450270>"
E_RED_ARROW = "<a:redarrow:1424106887103905892>"
E_STAR = "<a:yellowstar:1431126619795488791>"
E_AUCTION = "<:Auction:1392502904677601503>"
E_BOOK = "<a:PT_rules:954258166554693633>"
E_NYAN = "<a:NyanCat:1431353591268114452>"

# ---------- MONGODB CONNECTION ----------
if not MONGO_URL:
    print("CRITICAL ERROR: MONGO_URL environment variable is missing.")

cluster = MongoClient(MONGO_URL)
db = cluster["auction_bot"] 

# Collections
clubs_col = db.clubs
duelists_col = db.duelists
groups_col = db.investor_groups
group_members_col = db.groups_members
wallets_col = db.personal_wallets
profiles_col = db.user_profiles
bids_col = db.bids
history_col = db.club_history
contracts_col = db.duelist_contracts
battles_col = db.battle_register
audit_col = db.audit_logs
config_col = db.bot_config
counters_col = db.counters

def get_next_id(sequence_name):
    ret = counters_col.find_one_and_update(
        {"_id": sequence_name},
        {"$inc": {"seq": 1}},
        upsert=True,
        return_document=ReturnDocument.AFTER
    )
    return ret['seq']

# ---------- DISCORD BOT SETUP ----------
DEFAULT_PREFIX = "."

def get_prefix(bot, message):
    res = config_col.find_one({"key": "prefix"})
    prefix = res["value"] if res else DEFAULT_PREFIX
    return commands.when_mentioned_or(prefix)(bot, message)

intents = discord.Intents.default()
intents.message_content = True
intents.members = True

bot = commands.Bot(command_prefix=get_prefix, intents=intents)
active_timers = {}
bidding_frozen = False

# ---------- HELPER: EMBED BUILDER ----------
def create_embed(title, description, color=0x2ecc71, thumbnail=None, footer=None):
    embed = discord.Embed(title=title, description=description, color=color)
    if thumbnail and isinstance(thumbnail, str) and (thumbnail.startswith("http://") or thumbnail.startswith("https://")):
        embed.set_thumbnail(url=thumbnail)
    if footer:
        embed.set_footer(text=footer)
    return embed

# ---------- UTIL FUNCTIONS ----------
def log_audit(entry: str):
    audit_col.insert_one({"entry": entry, "timestamp": datetime.now()})

def get_current_bid(item_type=None, item_id=None):
    if item_type and item_id is not None:
        bid = bids_col.find_one({"item_type": item_type, "item_id": int(item_id)}, sort=[("amount", -1)])
        if bid: return bid["amount"]
    
    if item_type == "club":
        c = clubs_col.find_one({"id": int(item_id)})
        return c["base_price"] if c else 0
    if item_type == "duelist":
        d = duelists_col.find_one({"id": int(item_id)})
        return d["base_price"] if d else 0
    return 0

def min_required_bid(current):
    add = current * MIN_INCREMENT_PERCENT / 100
    return int(current + max(1, round(add)))

def get_club_owner_info(club_id):
    c = clubs_col.find_one({"id": int(club_id)})
    if not c or "owner_id" not in c or not c["owner_id"]:
        return None, []
    
    owner_str = c["owner_id"]
    if owner_str.startswith('group:'):
        gname = owner_str.replace('group:', '').lower()
        members = group_members_col.find({"group_name": gname})
        return owner_str, [m['user_id'] for m in members]
    else:
        return owner_str, [owner_str]

def get_level_info(current_wins, level_name=None):
    current_level = level_name if level_name else LEVEL_UP_CONFIG[0][1]
    next_level_info = None
    required_wins = 0
    for wins_required, name, bonus in LEVEL_UP_CONFIG:
        if wins_required > current_wins:
            next_level_info = (name, wins_required, bonus)
            required_wins = wins_required - current_wins
            break
        elif wins_required <= current_wins:
            current_level = name
    return current_level, next_level_info, required_wins

def update_club_level(club_id, wins_gained=0):
    c = clubs_col.find_one({"id": int(club_id)})
    if not c: return None
    new_wins = c.get("total_wins", 0) + wins_gained
    clubs_col.update_one({"id": int(club_id)}, {"$set": {"total_wins": new_wins}})
    for wins_required, name, bonus in LEVEL_UP_CONFIG:
        if c.get("total_wins", 0) < wins_required <= new_wins:
            clubs_col.update_one({"id": int(club_id)}, {"$set": {"level_name": name}, "$inc": {"value": bonus}})
            log_audit(f"Club {c['name']} leveled up to {name}. Bonus: {bonus}")
            return name
    return None

# ---------- BACKGROUND TASKS ----------
async def market_simulation_task():
    while True:
        await asyncio.sleep(3600)
        for c in clubs_col.find():
            base = c.get("value", c["base_price"])
            change = random.uniform(-0.03, 0.03)
            new_value = int(max(100, base * (1 + change)))
            clubs_col.update_one({"_id": c["_id"]}, {"$set": {"value": new_value}})

async def weekly_report_scheduler():
    while True:
        await asyncio.sleep(7 * 24 * 3600)
        pass

# ---------- AUCTION LOGIC ----------
async def finalize_auction(item_type: str, item_id: int, channel_id: int):
    winner_bid = bids_col.find_one({"item_type": item_type, "item_id": int(item_id)}, sort=[("amount", -1)])
    channel = bot.get_channel(channel_id)
    club_item = clubs_col.find_one({"id": int(item_id)}) if item_type == "club" else None
    
    if winner_bid:
        bidder_str = winner_bid["bidder"]
        amount = int(winner_bid["amount"])
        
        if bidder_str.startswith('group:'):
            gname = bidder_str.replace('group:', '').lower()
            groups_col.update_one({"name": gname}, {"$inc": {"funds": -amount}})
        else:
            wallets_col.update_one({"user_id": bidder_str}, {"$inc": {"balance": -amount}})
            
        if item_type == "club":
            history_col.insert_one({"club_id": int(item_id), "winner": bidder_str, "amount": amount, "timestamp": datetime.now(), "market_value_at_sale": club_item.get("value", 0)})
            clubs_col.update_one({"id": int(item_id)}, {"$set": {"owner_id": bidder_str, "last_bid_price": amount}})
            if not bidder_str.startswith('group:'):
                profiles_col.update_one({"user_id": bidder_str}, {"$set": {"owned_club_id": int(item_id), "owned_club_share": 100}}, upsert=True)
            if channel:
                await channel.send(embed=create_embed(f"{E_AUCTION} Auction Concluded", f"{E_SUCCESS} Winner: **{bidder_str}**\n{E_MONEY} Amount: **${amount:,}**\n{E_ITEMBOX} Item: **{club_item['name']}**", color=0xf1c40f))
        else: 
            d_item = duelists_col.find_one({"id": int(item_id)})
            salary = d_item["expected_salary"]
            contracts_col.insert_one({"duelist_id": int(item_id), "club_owner": bidder_str, "purchase_price": amount, "salary": salary, "signed_at": datetime.now()})
            
            target_club_id = None
            if bidder_str.startswith('group:'):
                gname = bidder_str.replace('group:', '').lower()
                c = clubs_col.find_one({"owner_id": f"group:{gname}"})
                if c: target_club_id = c['id']
            else:
                c = clubs_col.find_one({"owner_id": bidder_str})
                if c: target_club_id = c['id']
            
            duelists_col.update_one({"id": int(item_id)}, {"$set": {"owned_by": bidder_str, "club_id": target_club_id}})
            if channel:
                 await channel.send(embed=create_embed(f"{E_AUCTION} Duelist Signed", f"{E_SUCCESS} Signed To: **{bidder_str}**\n{E_MONEY} Fee: **${amount:,}**\n{E_ITEMBOX} Player: **{d_item['username']}**", color=0x9b59b6))
    else:
        if channel: await channel.send(embed=create_embed(f"{E_TIMER} Auction Ended", "No bids were placed.", color=0x95a5a6))
        
    bids_col.delete_many({"item_type": item_type, "item_id": int(item_id)})
    active_timers.pop((item_type, str(item_id)), None)

def schedule_auction_timer(item_type: str, item_id: int, channel_id: int):
    key = (item_type, str(item_id))
    if active_timers.get(key) and not active_timers.get(key).done(): active_timers[key].cancel()
    loop = asyncio.get_event_loop()
    t = loop.create_task(asyncio.sleep(TIME_LIMIT))
    async def wrapper():
        try: await t; await finalize_auction(item_type, item_id, channel_id)
        except asyncio.CancelledError: return
    active_timers[key] = loop.create_task(wrapper())

# ---------- ADMIN & SETUP ----------
@bot.command()
@commands.has_permissions(administrator=True)
async def registerclub(ctx, name: str, base_price: int, *, slogan: str = ""):
    if clubs_col.find_one({"name": {"$regex": f"^{name}$", "$options": "i"}}):
        return await ctx.send(embed=create_embed("Error", f"{E_ERROR} Club registered.", 0xff0000))
    logo_url = ctx.message.attachments[0].url if ctx.message.attachments else ""
    cid = get_next_id("club_id")
    clubs_col.insert_one({"id": cid, "name": name, "base_price": base_price, "value": base_price, "slogan": slogan, "logo": logo_url, "total_wins": 0, "level_name": LEVEL_UP_CONFIG[0][1], "owner_id": None, "manager_id": None})
    await ctx.send(embed=create_embed(f"{E_SUCCESS} Club Registered", f"{E_ARROW} **Name:** {name}\n{E_MONEY} **Base:** ${base_price:,}\n{E_ITEMBOX} **ID:** {cid}", 0x2ecc71, thumbnail=logo_url))

@bot.command()
@commands.has_permissions(administrator=True)
async def deleteclub(ctx, club_name: str):
    c = clubs_col.find_one({"name": {"$regex": f"^{club_name}$", "$options": "i"}})
    if not c: return await ctx.send(embed=create_embed("Error", "Club not found.", 0xff0000))
    clubs_col.delete_one({"id": c['id']})
    history_col.delete_many({"club_id": c['id']})
    duelists_col.update_many({"club_id": c['id']}, {"$set": {"club_id": None, "owned_by": None}})
    await ctx.send(embed=create_embed(f"{E_SUCCESS} Deleted", f"Club **{club_name}** removed.", 0xff0000))

@bot.command()
@commands.has_permissions(administrator=True)
async def setprefix(ctx, new_prefix: str):
    config_col.update_one({"key": "prefix"}, {"$set": {"value": new_prefix}}, upsert=True)
    await ctx.send(embed=create_embed(f"{E_SUCCESS} Prefix Updated", f"New prefix: **`{new_prefix}`**", 0x2ecc71))

@bot.command()
@commands.has_permissions(administrator=True)
async def admin_reset_all(ctx):
    if BOT_OWNER_ID and ctx.author.id != BOT_OWNER_ID: return
    await ctx.send(embed=create_embed(f"{E_DANGER} WARNING", "Resetting EVERYTHING...", 0xff0000))
    clubs_col.update_many({}, {"$set": {"total_wins": 0, "level_name": LEVEL_UP_CONFIG[0][1], "owner_id": None, "value": 1000000}})
    battles_col.delete_many({})
    history_col.delete_many({})
    await ctx.send(embed=create_embed(f"{E_SUCCESS} Reset", "System Reset Complete.", 0x2ecc71))

@bot.command()
@commands.has_permissions(administrator=True)
async def forcewinner(ctx, item_type: str, item_id: int, winner_str: str, amount: int):
    bids_col.insert_one({"bidder": winner_str, "amount": amount, "item_type": item_type, "item_id": int(item_id)})
    await finalize_auction(item_type, int(item_id), ctx.channel.id)
    await ctx.send(embed=create_embed(f"{E_ADMIN} Force Win", f"Forced winner **{winner_str}**.", 0xe67e22))

@bot.command()
@commands.has_permissions(administrator=True)
async def freezeauction(ctx):
    global bidding_frozen
    bidding_frozen = True
    await ctx.send(embed=create_embed(f"{E_DANGER} Frozen", "Auctions frozen.", 0xff0000))

@bot.command()
@commands.has_permissions(administrator=True)
async def unfreezeauction(ctx):
    global bidding_frozen
    bidding_frozen = False
    await ctx.send(embed=create_embed(f"{E_SUCCESS} Unfrozen", "Auctions resumed.", 0x2ecc71))

# ---------- AUCTION START ----------
@bot.command()
@commands.has_permissions(administrator=True)
async def startclubauction(ctx, club_name: str):
    c = clubs_col.find_one({"name": {"$regex": f"^{club_name}$", "$options": "i"}})
    if not c: return await ctx.send(embed=create_embed("Error", f"{E_ERROR} Club not found.", 0xff0000))
    bids_col.delete_many({"item_type": "club", "item_id": c["id"]})
    await ctx.send(embed=create_embed(f"{E_AUCTION} Auction Started", f"{E_ARROW} **Club:** {c['name']}\n{E_MONEY} **Base:** ${c['base_price']:,}", 0xe67e22, thumbnail=c.get('logo')))
    schedule_auction_timer("club", c["id"], ctx.channel.id)

@bot.command()
@commands.has_permissions(administrator=True)
async def startduelistauction(ctx, duelist_id: int):
    d = duelists_col.find_one({"id": int(duelist_id)})
    if not d: return await ctx.send(embed=create_embed("Error", f"{E_ERROR} Duelist not found.", 0xff0000))
    bids_col.delete_many({"item_type": "duelist", "item_id": d["id"]})
    await ctx.send(embed=create_embed(f"{E_AUCTION} Duelist Auction", f"{E_ARROW} **Player:** {d['username']}\n{E_MONEY} **Base:** ${d['base_price']:,}", 0x9b59b6, thumbnail=d.get('avatar_url')))
    schedule_auction_timer("duelist", d["id"], ctx.channel.id)

# ---------- USER COMMANDS ----------
@bot.command()
async def listclubs(ctx):
    clubs = list(clubs_col.find().sort("value", -1).limit(20))
    embed = discord.Embed(title=f"{E_CROWN} Registered Clubs", color=0x3498db)
    for c in clubs:
        embed.add_field(name=f"{E_STAR} {c['name']} (ID: {c['id']})", value=f"{E_MONEY} ${c['value']:,} | {E_BOOST} {c.get('level_name')} | {E_FIRE} Wins: {c.get('total_wins',0)}", inline=False)
    await ctx.send(embed=embed)

@bot.command()
async def listduelists(ctx):
    ds = list(duelists_col.find().limit(25))
    embed = discord.Embed(title=f"{E_BOOK} Duelist Registry", color=0x9b59b6)
    for d in ds:
        cname = "Free Agent"
        if d.get("club_id"):
            c = clubs_col.find_one({"id": d["club_id"]})
            if c: cname = c["name"]
        embed.add_field(name=f"{d['username']}", value=f"{E_ITEMBOX} ID: {d['id']}\n{E_MONEY} ${d['expected_salary']:,}\n{E_STAR} {cname}", inline=True)
    await ctx.send(embed=embed)

@bot.command()
async def clubinfo(ctx, *, club_name_or_id: str):
    try: c = clubs_col.find_one({"id": int(club_name_or_id)})
    except: c = clubs_col.find_one({"name": {"$regex": f"^{club_name_or_id}$", "$options": "i"}})
    if not c: return await ctx.send(embed=create_embed("Error", f"{E_ERROR} Club not found.", 0xff0000))
    
    owner_display = c.get('owner_id') or "Unowned"
    if owner_display.startswith('group:'): owner_display = f"Group: {owner_display.replace('group:', '').title()}"
    else:
        try:
            if owner_display != "Unowned":
                owner_user = await bot.fetch_user(int(owner_display))
                owner_display = f"User: {owner_user.display_name}"
        except: pass
    
    manager_name = "None"
    if c['manager_id']:
        try: u = await bot.fetch_user(int(c['manager_id'])); manager_name = u.name
        except: pass
    
    duelists = list(duelists_col.find({"club_id": c['id']}))
    d_list = "\n".join([f"{E_ARROW} {d['username']}" for d in duelists]) or "None"
    
    embed = discord.Embed(title=f"{E_CROWN} {c['name']}", description=f"{E_BOOST} **{c.get('level_name')}**", color=0x3498db)
    if c.get("logo") and c["logo"].startswith("http"): embed.set_thumbnail(url=c["logo"])
    embed.add_field(name="Owner", value=f"{E_STAR} {owner_display}", inline=True)
    embed.add_field(name="Value", value=f"{E_MONEY} ${c['value']:,}", inline=True)
    embed.add_field(name="Manager", value=f"{E_ADMIN} {manager_name}", inline=True)
    embed.add_field(name=f"{E_BOX} Duelists", value=d_list, inline=False)
    await ctx.send(embed=embed)

@bot.command()
async def clublevel(ctx, *, club_name_or_id: str):
    try: c = clubs_col.find_one({"id": int(club_name_or_id)})
    except: c = clubs_col.find_one({"name": {"$regex": f"^{club_name_or_id}$", "$options": "i"}})
    if not c: return await ctx.send(embed=create_embed("Error", "Club not found.", 0xff0000))
    
    cur, nxt, req = get_level_info(c.get('total_wins', 0), c.get('level_name'))
    embed = create_embed(f"{E_BOOST} Club Level", f"**{c['name']}**\n{E_CROWN} Current: **{cur}**\n{E_FIRE} Wins: **{c.get('total_wins',0)}**", 0xf1c40f)
    if nxt: embed.add_field(name="Next Level", value=f"{E_ARROW} **{nxt[0]}**\n{E_RED_ARROW} Needs **{req}** wins")
    else: embed.add_field(name="Status", value=f"{E_GOLD_TICK} Max Level")
    await ctx.send(embed=embed)

@bot.command()
async def marketpanel(ctx, *, club_name_or_id: str):
    try: c = clubs_col.find_one({"id": int(club_name_or_id)})
    except: c = clubs_col.find_one({"name": {"$regex": f"^{club_name_or_id}$", "$options": "i"}})
    if not c: return await ctx.send(embed=create_embed("Error", f"{E_ERROR} Club not found.", 0xff0000))
    
    cur_lvl, nxt_lvl, req_wins = get_level_info(c.get('total_wins', 0), c.get('level_name'))
    embed = discord.Embed(title=f"{E_STARS} Market Panel: {c['name']}", color=0xf1c40f)
    if c.get("logo") and c["logo"].startswith("http"): embed.set_thumbnail(url=c["logo"])
    embed.add_field(name="Market Value", value=f"{E_MONEY} **${c['value']:,}**", inline=True)
    embed.add_field(name="Total Wins", value=f"{E_FIRE} **{c.get('total_wins', 0)}**", inline=True)
    embed.add_field(name="Division", value=f"{E_CROWN} **{cur_lvl}**", inline=False)
    if nxt_lvl: embed.add_field(name="Next", value=f"{E_ARROW} **{nxt_lvl[0]}** (Req: {req_wins} wins)", inline=True)
    else: embed.add_field(name="Status", value=f"{E_GOLD_TICK} Max Level", inline=False)
    await ctx.send(embed=embed)

@bot.command()
async def leaderboard(ctx):
    clubs = list(clubs_col.find().sort([("total_wins", -1), ("value", -1)]).limit(15))
    embed = discord.Embed(title=f"{E_CROWN} Club Leaderboard", color=0xf1c40f)
    desc = ""
    for i, c in enumerate(clubs):
        desc += f"**{i+1}. {c['name']}**\n{E_ARROW} {c.get('level_name')} | {E_FIRE} {c.get('total_wins')} Wins | {E_MONEY} ${c['value']:,}\n\n"
    embed.description = desc
    await ctx.send(embed=embed)

# ---------- WALLET & GROUP COMMANDS ----------
@bot.command()
async def wallet(ctx):
    w = wallets_col.find_one({"user_id": str(ctx.author.id)})
    bal = w["balance"] if w else 0
    embed = create_embed(
        f"{E_MONEY} {E_NYAN} Wallet Balance",
        f"**User:** {ctx.author.mention}\n**Cash:** ${bal:,}",
        0x2ecc71, thumbnail=ctx.author.avatar.url if ctx.author.avatar else None
    )
    await ctx.send(embed=embed)

@bot.command()
async def withdrawwallet(ctx, amount: int):
    if amount <= 0: return await ctx.send(embed=create_embed("Error", "Invalid amount.", 0xff0000))
    w = wallets_col.find_one({"user_id": str(ctx.author.id)})
    if not w or w.get("balance", 0) < amount: return await ctx.send(embed=create_embed("Error", "Insufficient funds.", 0xff0000))
    wallets_col.update_one({"user_id": str(ctx.author.id)}, {"$inc": {"balance": -amount}})
    await ctx.send(embed=create_embed(f"{E_SUCCESS} Withdrawn", f"Removed **${amount:,}** from wallet.", 0x2ecc71))

@bot.command()
async def groupinfo(ctx, *, group_name: str):
    gname = group_name.lower()
    g = groups_col.find_one({"name": gname})
    if not g: return await ctx.send(embed=create_embed("Error", f"{E_ERROR} Group not found.", 0xff0000))
    
    members = list(group_members_col.find({"group_name": gname}))
    clubs = list(clubs_col.find({"owner_id": f"group:{gname}"}))
    
    embed = discord.Embed(title=f"{E_PREMIUM} Group: {g['name'].title()}", color=0x9b59b6)
    embed.add_field(name="Bank", value=f"{E_MONEY} ${g['funds']:,}", inline=True)
    
    mlist = []
    for m in members:
        try: u = await bot.fetch_user(int(m['user_id'])); name = u.name
        except: name = "Unknown"
        mlist.append(f"{E_ARROW} {name}: {m['share_percentage']}%")
        
    embed.add_field(name=f"Members ({len(members)})", value="\n".join(mlist) or "None", inline=False)
    clist = [c['name'] for c in clubs]
    embed.add_field(name="Clubs Owned", value=", ".join(clist) or "None", inline=False)
    await ctx.send(embed=embed)

@bot.command()
async def creategroup(ctx, name: str, share: int):
    gname = name.lower()
    if groups_col.find_one({"name": gname}): return await ctx.send(embed=create_embed("Error", "Group exists.", 0xff0000))
    groups_col.insert_one({"name": gname, "funds": 0, "owner_id": str(ctx.author.id)})
    group_members_col.insert_one({"group_name": gname, "user_id": str(ctx.author.id), "share_percentage": share})
    await ctx.send(embed=create_embed(f"{E_SUCCESS} Group Created", f"Group **{name}** created with **{share}%** share.", 0x2ecc71))

@bot.command()
async def joingroup(ctx, name: str, share: int):
    gname = name.lower()
    if not groups_col.find_one({"name": gname}): return await ctx.send(embed=create_embed("Error", "Group not found.", 0xff0000))
    if group_members_col.find_one({"group_name": gname, "user_id": str(ctx.author.id)}):
        return await ctx.send(embed=create_embed("Error", "Already a member.", 0xff0000))
    group_members_col.insert_one({"group_name": gname, "user_id": str(ctx.author.id), "share_percentage": share})
    await ctx.send(embed=create_embed(f"{E_SUCCESS} Joined", f"Joined **{name}** with **{share}%**.", 0x2ecc71))

@bot.command()
async def deposit(ctx, group_name: str, amount: int):
    if amount <= 0: return
    gname = group_name.lower()
    if not group_members_col.find_one({"group_name": gname, "user_id": str(ctx.author.id)}):
        return await ctx.send(embed=create_embed("Error", f"{E_ERROR} Not a member.", 0xff0000))
        
    w = wallets_col.find_one({"user_id": str(ctx.author.id)})
    if not w or w.get("balance", 0) < amount:
        return await ctx.send(embed=create_embed("Error", f"{E_ERROR} Insufficient funds.", 0xff0000))
        
    wallets_col.update_one({"user_id": str(ctx.author.id)}, {"$inc": {"balance": -amount}})
    groups_col.update_one({"name": gname}, {"$inc": {"funds": amount}})
    await ctx.send(embed=create_embed(f"{E_SUCCESS} Deposit", f"Deposited **${amount:,}** to **{group_name}**.", 0x2ecc71))

@bot.command()
async def sellclub(ctx, club_name: str, buyer: discord.Member = None):
    c = clubs_col.find_one({"name": {"$regex": f"^{club_name}$", "$options": "i"}})
    if not c: return await ctx.send(embed=create_embed("Error", "Club not found.", 0xff0000))
    if str(ctx.author.id) != c.get("owner_id"): return await ctx.send(embed=create_embed("Error", "You don't own this.", 0xff0000))
    
    val = c["value"]
    target = buyer if buyer else "The Market"
    await ctx.send(embed=create_embed(f"{E_ALERT} Confirm Sale", f"Sell **{c['name']}** to {target.mention if buyer else 'Market'} for **${val:,}**?\nType `yes` or `no`.", 0xe67e22))
    
    def check(m): return m.author == (buyer if buyer else ctx.author) and m.content.lower() in ['yes', 'no']
    try: msg = await bot.wait_for('message', check=check, timeout=30)
    except: return await ctx.send(embed=create_embed("Info", "Timed out.", 0x95a5a6))
    if msg.content.lower() == 'no': return await ctx.send(embed=create_embed("Info", "Cancelled.", 0x95a5a6))
    
    if buyer:
        bw = wallets_col.find_one({"user_id": str(buyer.id)})
        if not bw or bw.get("balance", 0) < val: return await ctx.send(embed=create_embed("Error", "Buyer broke.", 0xff0000))
        wallets_col.update_one({"user_id": str(buyer.id)}, {"$inc": {"balance": -val}})
        clubs_col.update_one({"id": c["id"]}, {"$set": {"owner_id": str(buyer.id)}})
        profiles_col.update_one({"user_id": str(buyer.id)}, {"$set": {"owned_club_id": c["id"], "owned_club_share": 100}}, upsert=True)
        
        # FIX: Remove ownership from seller profile
        profiles_col.update_one({"user_id": str(ctx.author.id)}, {"$unset": {"owned_club_id": "", "owned_club_share": ""}})
    else:
        clubs_col.update_one({"id": c["id"]}, {"$set": {"owner_id": None}})
        # FIX: Remove ownership from seller profile
        profiles_col.update_one({"user_id": str(ctx.author.id)}, {"$unset": {"owned_club_id": "", "owned_club_share": ""}})
        
    wallets_col.update_one({"user_id": str(ctx.author.id)}, {"$inc": {"balance": val}}, upsert=True)
    await ctx.send(embed=create_embed(f"{E_SUCCESS} Sold", f"Club sold for **${val:,}**.", 0x2ecc71))

@bot.command()
async def sellshares(ctx, club_name: str, buyer: discord.Member, percentage: int):
    c = clubs_col.find_one({"name": {"$regex": f"^{club_name}$", "$options": "i"}})
    if not c: return await ctx.send(embed=create_embed("Error", "Club not found.", 0xff0000))
    owner_str = c.get("owner_id", "")
    if not owner_str.startswith("group:"): return await ctx.send(embed=create_embed("Error", "Not group owned.", 0xff0000))
    gname = owner_str.replace("group:", "")
    
    seller = group_members_col.find_one({"group_name": gname, "user_id": str(ctx.author.id)})
    if not seller or seller["share_percentage"] < percentage: return await ctx.send(embed=create_embed("Error", "Not enough shares.", 0xff0000))
    
    val = int(c["value"] * (percentage / 100))
    await ctx.send(embed=create_embed(f"{E_ALERT} Confirm Share Sale", f"{buyer.mention}, buy **{percentage}%** shares for **${val:,}**? `yes`/`no`", 0xe67e22))
    
    def check(m): return m.author == buyer and m.content.lower() in ['yes', 'no']
    try: msg = await bot.wait_for('message', check=check, timeout=30)
    except: return await ctx.send(embed=create_embed("Info", "Timed out.", 0x95a5a6))
    
    if msg.content.lower() == 'yes':
        bw = wallets_col.find_one({"user_id": str(buyer.id)})
        if not bw or bw.get("balance", 0) < val: return await ctx.send(embed=create_embed("Error", "Buyer broke.", 0xff0000))
        
        wallets_col.update_one({"user_id": str(buyer.id)}, {"$inc": {"balance": -val}})
        wallets_col.update_one({"user_id": str(ctx.author.id)}, {"$inc": {"balance": val}}, upsert=True)
        
        group_members_col.update_one({"_id": seller["_id"]}, {"$inc": {"share_percentage": -percentage}})
        group_members_col.update_one({"group_name": gname, "user_id": str(buyer.id)}, {"$inc": {"share_percentage": percentage}}, upsert=True)
        await ctx.send(embed=create_embed(f"{E_SUCCESS} Sold", "Shares transferred.", 0x2ecc71))

# ---------- ADMIN ----------
@bot.command()
@commands.has_permissions(administrator=True)
async def tip(ctx, member: discord.Member, amount: int):
    wallets_col.update_one({"user_id": str(member.id)}, {"$inc": {"balance": amount}}, upsert=True)
    await ctx.send(embed=create_embed(f"{E_ADMIN} Admin Tip", f"Added **${amount:,}** to {member.mention}.", 0xe67e22))

@bot.command()
@commands.has_permissions(administrator=True)
async def deduct_user(ctx, member: discord.Member, amount: int):
    wallets_col.update_one({"user_id": str(member.id)}, {"$inc": {"balance": -amount}}, upsert=True)
    await ctx.send(embed=create_embed(f"{E_ADMIN} Admin Deduct", f"Removed **${amount:,}** from {member.mention}.", 0xff0000))

@bot.command()
@commands.has_permissions(administrator=True)
async def setclubmanager(ctx, club_name: str, member: discord.Member):
    clubs_col.update_one({"name": {"$regex": f"^{club_name}$", "$options": "i"}}, {"$set": {"manager_id": str(member.id)}})
    await ctx.send(embed=create_embed(f"{E_SUCCESS} Manager Set", f"{member.mention} is now manager of {club_name}.", 0x2ecc71))

@bot.command()
@commands.has_permissions(administrator=True)
async def setprefix(ctx, new_prefix: str):
    config_col.update_one({"key": "prefix"}, {"$set": {"value": new_prefix}}, upsert=True)
    await ctx.send(embed=create_embed(f"{E_SUCCESS} Prefix Updated", f"New prefix: **`{new_prefix}`**", 0x2ecc71))

@bot.command()
@commands.has_permissions(administrator=True)
async def registerbattle(ctx, club_a_name: str, club_b_name: str):
    ca = clubs_col.find_one({"name": {"$regex": f"^{club_a_name}$", "$options": "i"}})
    cb = clubs_col.find_one({"name": {"$regex": f"^{club_b_name}$", "$options": "i"}})
    if not ca or not cb: return await ctx.send(embed=create_embed("Error", "Clubs not found.", 0xff0000))
    bid = get_next_id("battle_id")
    battles_col.insert_one({"id": bid, "club_a": ca['id'], "club_b": cb['id'], "status": "REGISTERED"})
    await ctx.send(embed=create_embed(f"{E_FIRE} Battle Ready", f"**{ca['name']}** vs **{cb['name']}**\nID: {bid}", 0xe74c3c))

@bot.command()
@commands.has_permissions(administrator=True)
async def battleresult(ctx, battle_id: int, winner_name: str):
    b = battles_col.find_one({"id": int(battle_id)})
    if not b: return await ctx.send(embed=create_embed("Error", "Battle not found.", 0xff0000))
    wc = clubs_col.find_one({"name": {"$regex": f"^{winner_name}$", "$options": "i"}})
    if not wc: return await ctx.send(embed=create_embed("Error", "Winner not found.", 0xff0000))
    
    loser_id = b['club_a'] if b['club_b'] == wc['id'] else b['club_b']
    clubs_col.update_one({"id": wc['id']}, {"$inc": {"value": WIN_VALUE_BONUS}})
    clubs_col.update_one({"id": loser_id}, {"$inc": {"value": LOSS_VALUE_PENALTY}})
    battles_col.update_one({"id": int(battle_id)}, {"$set": {"status": "COMPLETED"}})
    update_club_level(wc['id'], 1)
    await ctx.send(embed=create_embed(f"{E_SUCCESS} Battle Recorded", f"Winner: **{wc['name']}** (+${WIN_VALUE_BONUS:,})", 0x2ecc71))

@bot.command()
@commands.has_permissions(administrator=True)
async def checkclubmessages(ctx, club_name: str, count: int):
    c = clubs_col.find_one({"name": {"$regex": f"^{club_name}$", "$options": "i"}})
    if not c: return await ctx.send(embed=create_embed("Error", "Club not found.", 0xff0000))
    bonus = (count // OWNER_MSG_COUNT_PER_BONUS) * OWNER_MSG_VALUE_BONUS
    if bonus > 0:
        clubs_col.update_one({"id": c['id']}, {"$inc": {"value": bonus}})
        await ctx.send(embed=create_embed(f"{E_BOOST} Activity Bonus", f"**{c['name']}** value increased by **${bonus:,}**.", 0x2ecc71))
    else: await ctx.send(embed=create_embed("Info", "Not enough messages.", 0x95a5a6))

@bot.command()
@commands.has_permissions(administrator=True)
async def adjustgroupfunds(ctx, group_name: str, amount: int):
    groups_col.update_one({"name": group_name.lower()}, {"$inc": {"funds": amount}})
    await ctx.send(embed=create_embed(f"{E_ADMIN} Funds Adjusted", f"Adjusted **{group_name}** by ${amount:,}.", 0xe67e22))

@bot.command()
async def auditlog(ctx, lines: int = 10):
    logs = list(audit_col.find().sort("timestamp", -1).limit(lines))
    txt = "\n".join([f"[{l['timestamp'].strftime('%H:%M')}] {l['entry']}" for l in logs])
    await ctx.send(f"```{txt}```")

@bot.command()
async def resetauction(ctx):
    bids_col.delete_many({})
    await ctx.send(embed=create_embed(f"{E_SUCCESS} Reset", "Bids cleared.", 0x2ecc71))

@bot.command()
@commands.has_permissions(administrator=True)
async def transferclub(ctx, old_grp: str, new_grp: str):
    c = clubs_col.find_one({"owner_id": f"group:{old_grp.lower()}"})
    if c:
        clubs_col.update_one({"id": c['id']}, {"$set": {"owner_id": f"group:{new_grp.lower()}"}})
        await ctx.send(embed=create_embed(f"{E_ADMIN} Transferred", f"Club transferred to {new_grp}.", 0xe67e22))
    else: await ctx.send(embed=create_embed("Error", "No club found.", 0xff0000))

# ---------- USER ACTIONS ----------
@bot.command()
async def placebid(ctx, amount: int, item_type: str, item_id: int, club_name: str = None):
    if bidding_frozen: return await ctx.send(embed=create_embed("Frozen", f"{E_DANGER} Auctions frozen.", 0xff0000))
    item_type = item_type.lower()
    if item_type == "duelist":
        if not club_name: return await ctx.send(embed=create_embed("Error", "Provide club name.", 0xff0000))
        c = clubs_col.find_one({"name": {"$regex": f"^{club_name}$", "$options": "i"}})
        if not c:
            return await ctx.send(embed=create_embed("Error", "Club not found.", 0xff0000))
        
        # FIX: Allow Group Members to bid personally for Group Club
        allowed = False
        if str(ctx.author.id) == c.get("owner_id"):
            allowed = True
        elif c.get("owner_id", "").startswith("group:"):
            gname = c.get("owner_id").replace("group:", "")
            if group_members_col.find_one({"group_name": gname, "user_id": str(ctx.author.id)}):
                allowed = True
                
        if not allowed:
             return await ctx.send(embed=create_embed("Error", "You/Group don't own this club.", 0xff0000))
    
    w = wallets_col.find_one({"user_id": str(ctx.author.id)})
    # FIX: Handle None wallet gracefully
    if not w or w.get("balance", 0) < amount: return await ctx.send(embed=create_embed("Error", "Insufficient funds.", 0xff0000))
    
    req = min_required_bid(get_current_bid(item_type, item_id))
    if amount < req: return await ctx.send(embed=create_embed("Bid Error", f"Min bid is ${req:,}", 0xff0000))
    
    bids_col.insert_one({"bidder": str(ctx.author.id), "amount": amount, "item_type": item_type, "item_id": int(item_id), "timestamp": datetime.now()})
    await ctx.send(embed=create_embed(f"{E_SUCCESS} Bid Placed", f"Bid of **${amount:,}** accepted.", 0x2ecc71))
    schedule_auction_timer(item_type, item_id, ctx.channel.id)

@bot.command()
async def groupbid(ctx, group_name: str, amount: int, item_type: str, item_id: int, club_name: str = None):
    if bidding_frozen: return await ctx.send(embed=create_embed("Frozen", "Auctions frozen.", 0xff0000))
    gname = group_name.lower()
    g = groups_col.find_one({"name": gname})
    if not g: return await ctx.send(embed=create_embed("Error", "Group not found.", 0xff0000))
    if not group_members_col.find_one({"group_name": gname, "user_id": str(ctx.author.id)}): return await ctx.send(embed=create_embed("Error", "Not member.", 0xff0000))
    
    if item_type == "duelist":
        if not club_name: return await ctx.send(embed=create_embed("Error", "Provide club name.", 0xff0000))
        c = clubs_col.find_one({"name": {"$regex": f"^{club_name}$", "$options": "i"}})
        if not c or c.get("owner_id") != f"group:{gname}": return await ctx.send(embed=create_embed("Error", "Group doesn't own club.", 0xff0000))
        
    if g["funds"] < amount: return await ctx.send(embed=create_embed("Error", "Insufficient funds.", 0xff0000))
    
    bids_col.insert_one({"bidder": f"group:{gname}", "amount": amount, "item_type": item_type, "item_id": int(item_id)})
    await ctx.send(embed=create_embed(f"{E_SUCCESS} Group Bid", f"Group **{group_name}** bid **${amount:,}**.", 0x2ecc71))
    schedule_auction_timer(item_type, item_id, ctx.channel.id)

@bot.command()
async def registerduelist(ctx, username: str, base_price: int, salary: int):
    if duelists_col.find_one({"discord_user_id": str(ctx.author.id)}): return await ctx.send(embed=create_embed("Error", "Already registered.", 0xff0000))
    did = get_next_id("duelist_id")
    avatar = ctx.author.avatar.url if ctx.author.avatar else ""
    duelists_col.insert_one({"id": did, "discord_user_id": str(ctx.author.id), "username": username, "base_price": base_price, "expected_salary": salary, "avatar_url": avatar, "owned_by": None, "club_id": None})
    await ctx.send(embed=create_embed(f"{E_SUCCESS} Registered", f"Duelist **{username}** (ID: {did})", 0x9b59b6))

@bot.command()
async def retireduelist(ctx, member: discord.Member = None):
    target_id = str(member.id) if member else str(ctx.author.id)
    d = duelists_col.find_one({"discord_user_id": target_id})
    if not d: return await ctx.send(embed=create_embed("Error", "Not a duelist.", 0xff0000))
    
    if member:
        if not d.get("club_id"): return await ctx.send(embed=create_embed("Error", "Free agent must self-retire.", 0xff0000))
        c = clubs_col.find_one({"id": d["club_id"]})
        owner_str, owner_ids = get_club_owner_info(c["id"])
        if str(ctx.author.id) not in owner_ids: return await ctx.send(embed=create_embed("Error", "Not owner.", 0xff0000))
        
        await ctx.send(embed=create_embed(f"{E_ALERT} Confirm", f"Owner {ctx.author.mention}, retire {member.mention}? `yes`/`no`", 0xe67e22))
        try: 
            msg = await bot.wait_for('message', check=lambda m: m.author==ctx.author and m.content.lower() in ['yes','no'], timeout=30)
            if msg.content.lower() == 'no': return await ctx.send(embed=create_embed("Info", "Cancelled.", 0x95a5a6))
        except: return
        
        await ctx.send(embed=create_embed(f"{E_ALERT} Confirm", f"Duelist {member.mention}, confirm retirement? `yes`/`no`", 0xe67e22))
        try:
            msg2 = await bot.wait_for('message', check=lambda m: m.author==member and m.content.lower() in ['yes','no'], timeout=30)
            if msg2.content.lower() == 'no': return await ctx.send(embed=create_embed("Info", "Cancelled by duelist.", 0x95a5a6))
        except: return
    else:
        if d.get("owned_by"): return await ctx.send(embed=create_embed("Error", "You are signed. Ask owner.", 0xff0000))
        
    duelists_col.delete_one({"_id": d["_id"]})
    await ctx.send(embed=create_embed(f"{E_DANGER} Retired", f"Duelist **{d['username']}** retired.", 0xff0000))

@bot.command()
async def adjustsalary(ctx, duelist_id: int, amount: int):
    d = duelists_col.find_one({"id": int(duelist_id)})
    if not d or not d.get("club_id"): return await ctx.send(embed=create_embed("Error", "Duelist not found/signed.", 0xff0000))
    
    c = clubs_col.find_one({"id": d["club_id"]})
    owner_str, owner_ids = get_club_owner_info(c["id"])
    if str(ctx.author.id) not in owner_ids: return await ctx.send(embed=create_embed("Error", "Not owner.", 0xff0000))
    
    if amount > 0:
        w = wallets_col.find_one({"user_id": str(ctx.author.id)})
        if not w or w.get("balance", 0) < amount: return await ctx.send(embed=create_embed("Error", "Insufficient funds.", 0xff0000))
        wallets_col.update_one({"user_id": str(ctx.author.id)}, {"$inc": {"balance": -amount}})
        wallets_col.update_one({"user_id": d["discord_user_id"]}, {"$inc": {"balance": amount}}, upsert=True)
        await ctx.send(embed=create_embed(f"{E_MONEY} Bonus", f"Paid **${amount:,}** to {d['username']}.", 0x2ecc71))
    else:
        abs_amt = abs(amount)
        # FIX: Check duelist funds before deduction? Usually fines go through even if 0.
        wallets_col.update_one({"user_id": d["discord_user_id"]}, {"$inc": {"balance": -abs_amt}}, upsert=True)
        await ctx.send(embed=create_embed(f"{E_DANGER} Fine", f"Deducted **${abs_amt:,}** from {d['username']}.", 0xff0000))

@bot.command()
async def deductsalary(ctx, duelist_id: int, confirm: str):
    if confirm.lower() != "yes": return
    d = duelists_col.find_one({"id": int(duelist_id)})
    if not d: return await ctx.send(embed=create_embed("Error", "Duelist not found.", 0xff0000))
    
    # FIX: Validate Owner Logic
    if not d.get('club_id'): return await ctx.send(embed=create_embed("Error", "Duelist not in a club.", 0xff0000))
    owner_str, owner_ids = get_club_owner_info(d['club_id'])
    if str(ctx.author.id) not in owner_ids and not ctx.author.guild_permissions.administrator:
         return await ctx.send(embed=create_embed("Error", "Not authorized.", 0xff0000))

    penalty = int(d["expected_salary"] * (DUELIST_MISS_PENALTY_PERCENT / 100))
    # FIX: Deduct from Duelist
    wallets_col.update_one({"user_id": d["discord_user_id"]}, {"$inc": {"balance": -penalty}}, upsert=True)
    await ctx.send(embed=create_embed(f"{E_ALERT} Penalty", f"Fined **${penalty:,}** from **{d['username']}**'s wallet.", 0xff0000))

@bot.command()
async def depositwallet(ctx, amount: int = None):
    await ctx.send(embed=create_embed(f"{E_DANGER} Restricted", "Use `.deposit <Group> <Amt>` to fund group.", 0xff0000))

@bot.command()
async def withdraw(ctx, group_name: str, amount: int):
    gname = group_name.lower()
    if not group_members_col.find_one({"group_name": gname, "user_id": str(ctx.author.id)}): return await ctx.send(embed=create_embed("Error", "Not member.", 0xff0000))
    g = groups_col.find_one({"name": gname})
    if g["funds"] < amount: return await ctx.send(embed=create_embed("Error", "Insufficient funds.", 0xff0000))
    groups_col.update_one({"name": gname}, {"$inc": {"funds": -amount}})
    wallets_col.update_one({"user_id": str(ctx.author.id)}, {"$inc": {"balance": amount}})
    await ctx.send(embed=create_embed(f"{E_SUCCESS} Withdraw", f"Withdrew **${amount:,}**.", 0x2ecc71))

@bot.command()
async def leavegroup(ctx, name: str):
    gname = name.lower()
    mem = group_members_col.find_one({"group_name": gname, "user_id": str(ctx.author.id)})
    if not mem: return await ctx.send(embed=create_embed("Error", "Not a member.", 0xff0000))
    if mem['share_percentage'] > 0: return await ctx.send(embed=create_embed("Error", "Sell shares first.", 0xff0000))
    
    g = groups_col.find_one({"name": gname})
    penalty = int(g["funds"] * (LEAVE_PENALTY_PERCENT / 100))
    groups_col.update_one({"name": gname}, {"$inc": {"funds": -penalty}})
    group_members_col.delete_one({"_id": mem["_id"]})
    await ctx.send(embed=create_embed(f"{E_DANGER} Left Group", f"Left **{name}**. Penalty: **${penalty:,}**.", 0xff0000))

# ---------- HELP ----------
@bot.command()
async def helpme(ctx):
    p = get_prefix(bot, ctx.message)
    
    e1 = discord.Embed(title=f"{E_MONEY} User & Economy", color=0x2ecc71)
    e1.add_field(name="Basic", value=f"`{p}profile`\n`{p}wallet`\n`{p}withdrawwallet`", inline=True)
    e1.add_field(name="Group", value=f"`{p}creategroup`\n`{p}joingroup`\n`{p}deposit`\n`{p}groupinfo`", inline=True)
    await ctx.send(embed=e1)
    
    e2 = discord.Embed(title=f"{E_AUCTION} Market & Bids", color=0xe67e22)
    e2.add_field(name="Bidding", value=f"`{p}placebid`\n`{p}groupbid`\n`{p}sellclub`\n`{p}sellshares`", inline=True)
    e2.add_field(name="Info", value=f"`{p}marketpanel`\n`{p}clubinfo`\n`{p}leaderboard`\n`{p}listclubs`", inline=True)
    await ctx.send(embed=e2)
    
    e3 = discord.Embed(title=f"{E_ADMIN} Admin Tools", color=0x95a5a6)
    e3.add_field(name="Setup", value=f"`{p}registerclub`\n`{p}startclubauction`\n`{p}setprefix`", inline=True)
    e3.add_field(name="Actions", value=f"`{p}registerbattle`\n`{p}battleresult`\n`{p}tip`\n`{p}deduct_user`", inline=True)
    e3.add_field(name="Owner", value=f"`{p}admin_reset_all`\n`{p}forcewinner`\n`{p}freezeauction`\n`{p}adjustsalary`\n`{p}deductsalary`", inline=True)
    await ctx.send(embed=e3)

@bot.command()
async def profile(ctx, member: discord.Member = None):
    member = member or ctx.author
    uid = str(member.id)
    w = wallets_col.find_one({"user_id": uid})
    bal = w["balance"] if w else 0
    
    embed = create_embed(f"{E_CROWN} Profile", f"**User:** {member.mention}\n{E_MONEY} **Balance:** ${bal:,}", 0x3498db, thumbnail=member.avatar.url if member.avatar else None)
    
    # Get Groups
    groups = list(group_members_col.find({"user_id": uid}))
    g_list = [f"{g['group_name'].title()} ({g['share_percentage']}%)" for g in groups]
    embed.add_field(name="Groups", value=", ".join(g_list) if g_list else "None", inline=False)
    
    # Get Recent Bids
    bids = list(bids_col.find({"bidder": uid}).sort("timestamp", -1).limit(5))
    b_list = [f"{b['item_type'].title()} ID {b['item_id']}: ${b['amount']:,}" for b in bids]
    embed.add_field(name="Recent Bids", value="\n".join(b_list) if b_list else "None", inline=False)
    
    # Owned Club (Solo)
    prof = profiles_col.find_one({"user_id": uid})
    if prof and prof.get("owned_club_id"):
        c = clubs_col.find_one({"id": prof["owned_club_id"]})
        if c: embed.add_field(name="Owned Club", value=f"{c['name']} (100%)", inline=False)
    
    # Duelist Info
    duelist = duelists_col.find_one({"discord_user_id": uid})
    if duelist:
        cname = "Free Agent"
        if duelist.get("club_id"):
            c = clubs_col.find_one({"id": duelist["club_id"]})
            if c: cname = c["name"]
        embed.add_field(name="Duelist Status", value=f"Club: {cname}\nSalary: ${duelist['expected_salary']:,}", inline=False)

    await ctx.send(embed=embed)

# ---------- RUN ----------
@bot.event
async def on_ready():
    print(f"Logged in as {bot.user}")
    if not hasattr(bot, 'started'):
        bot.loop.create_task(market_simulation_task())
        bot.started = True

if __name__ == "__main__":
    bot.run(DISCORD_TOKEN)