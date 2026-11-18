=LET(

TxnTime,TEXT(C2,"dd-mmm-yy")&" at "&TEXT(C2,"hh:mmAM/PM"),

CustomerName,LEFT(P2,FIND(" ",P2&" ")-1),

PartyDetails,
"• Kes "&TEXT(G2,"#,##0")&
" - (Transaction ID - "&A2&") - "&U2&" "&V2&
" of ID NO: "&Y2&
" and MSISDN "&H2&
", was born on "&IF(X2="","N/A",TEXT(X2,"dd-mmm-yy"))&
", and got registered on Airtel Money on "&IF(W2="","N/A",TEXT(W2,"dd-mmm-yy")),

IF(TRIM(UPPER(F2))="IMTIN",

"On "&TxnTime&", "&CustomerName&
" received an amount of Kes "&TEXT(G2,"#,##0")&
" via IMT Thunes, bringing the balance to Kes "&TEXT(M2,"#,##0")&
" with details as shown below:"&CHAR(10)&
"• Kes "&TEXT(G2,"#,##0")&
" - (Transaction ID - "&A2&") - IMT incoming transaction from "&
MID(N2,SEARCH("senderFirstName"":""",N2)+LEN("senderFirstName"":"""),FIND("""",N2,SEARCH("senderFirstName"":""",N2)+LEN("senderFirstName"":"""))-(SEARCH("senderFirstName"":""",N2)+LEN("senderFirstName"":""")))&" "&
MID(N2,SEARCH("senderLastName"":""",N2)+LEN("senderLastName"":"""),FIND("""",N2,SEARCH("senderLastName"":""",N2)+LEN("senderLastName"":"""))-(SEARCH("senderLastName"":""",N2)+LEN("senderLastName"":""")))&
" via THUNES.",

IF(TRIM(UPPER(F2))="SUBSCASHOUT",

"On "&TxnTime&", "&CustomerName&
" cashed out an amount of Kes "&TEXT(G2,"#,##0")&
" through an Airtel Money Agent leaving the balance at Kes "&TEXT(M2,"#,##0")&
", with details shown below:"&CHAR(10)&
PartyDetails,

IF(TRIM(UPPER(F2))="SUBSCASHIN",

"On "&TxnTime&", "&CustomerName&
" received a cash deposit of Kes "&TEXT(G2,"#,##0")&
" from an Airtel Money Agent bringing the balance to Kes "&TEXT(M2,"#,##0")&
", with details shown below:"&CHAR(10)&
PartyDetails,

IF(TRIM(UPPER(F2))="TXN_ROLLBACK",

"On "&TxnTime&", "&CustomerName&
" received an automated system reversal of Kes "&TEXT(G2,"#,##0")&
" bringing the balance to Kes "&TEXT(M2,"#,##0")&
", with details shown below:"&CHAR(10)&
"• Kes "&TEXT(G2,"#,##0")&
" - (Transaction ID - "&A2&") - Automated System Reversal.",

IF(TRIM(UPPER(F2))="BONUSDISB",

"On "&TxnTime&", "&CustomerName&
" received an amount of Kes "&TEXT(G2,"#,##0")&
" from Airtel Money Bonus on Transaction Disbursement Wallets bringing the balance to Kes "&TEXT(M2,"#,##0")&
", with details shown below:"&CHAR(10)&
"• Kes "&TEXT(G2,"#,##0")&
" - (Transaction ID - "&A2&") - Airtel Money Bonus on Transaction Disbursement Wallets.",

IF(TRIM(UPPER(F2))="SEW2W",

"On "&TxnTime&", "&CustomerName&
" moved an amount of Kes "&TEXT(G2,"#,##0")&
" from one Airtel Money wallet to another Airtel Money wallet, with details shown below:"&CHAR(10)&
PartyDetails,

IF(TRIM(UPPER(F2))="BUNSLF",

"On "&TxnTime&", "&CustomerName&
" bought bundles worth Kes "&TEXT(G2,"#,##0")&
" leaving the balance at Kes "&TEXT(M2,"#,##0")&
", with details shown below:"&CHAR(10)&
"• Kes "&TEXT(G2,"#,##0")&
" - (Transaction ID - "&A2&") - "&U2&" "&V2,

IF(TRIM(UPPER(F2))="P2PIOPIN",

"On "&TxnTime&", "&CustomerName&
" received an amount of Kes "&TEXT(G2,"#,##0")&
" from a Safaricom User bringing the balance to Kes "&TEXT(M2,"#,##0")&
", with details shown below:"&CHAR(10)&
"• Kes "&TEXT(G2,"#,##0")&
" - (Transaction ID - "&A2&") - "&U2&" "&V2,

IF(TRIM(UPPER(I2))="SUBS",

"On "&TxnTime&", "&CustomerName&
" sent an amount of Kes "&TEXT(G2,"#,##0")&
" to an Airtel Money User leaving the balance at "&TEXT(M2,"#,##0")&
", with the details shown below:"&CHAR(10)&
PartyDetails,

IF(ISNUMBER(SEARCH("subscriberMsisdn"":""",N2)),

"On "&TxnTime&", "&CustomerName&
" "&IF(O2="MR","received","sent")&
" an amount of Kes "&TEXT(G2,"#,##0")&
" to a Safaricom user with details shown below:"&CHAR(10)&
"• Kes "&TEXT(G2,"#,##0")&
" - (Transaction ID - "&A2&") - Sent to Safaricom user of MSISDN "&
MID(N2,SEARCH("subscriberMsisdn"":""",N2)+19,FIND("""",N2,SEARCH("subscriberMsisdn"":""",N2)+19)-(SEARCH("subscriberMsisdn"":""",N2)+19))&
" via MPESA.",

IF(ISNUMBER(SEARCH("Lipa Na Mpesa via Airtel Money",U2)),

"On "&TxnTime&", "&CustomerName&
" sent an amount of Kes "&TEXT(G2,"#,##0")&
" to a Safaricom Till, with the details shown below:"&CHAR(10)&
"• Kes "&TEXT(G2,"#,##0")&
" - (Transaction ID - "&A2&") - "&U2,

"On "&TxnTime&", "&CustomerName&
" "&IF(O2="MR","received","sent")&
" an amount of Kes "&TEXT(G2,"#,##0")&
" with the Airtel Money Transaction ID "&A2&CHAR(10)&
PartyDetails

))))))))))))

)
