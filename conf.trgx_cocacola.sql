 --**********************************
 --* [intro]
 --*   author=larluo@spiderdt.com
 --*   func=partition algorithm for data warehouse
 --*=================================
 --* [param]
 --*   tabname=staging table name
 --*   prt_cols_str=ods partition cols
 --*=================================
 --* [caller]
 --*   spark_etl_prt.groovy d_bolome_dau '[[":p_date", 1]]'
 --*   spark_etl_prt.groovy d_bolome_events
 --*   spark_etl_prt.groovy d_bolome_inventory
 --*   spark_etl_prt.groovy d_bolome_orders
 --*=================================
 --* [version]
 --*   v1_0=2016-09-28@larluo{create}
 --**********************************

  \c dw ;
  
  CREATE TABLE IF NOT EXISTS conf.trgx_cocacola (
    key TEXT,
    data TEXT,
    dw_in_use CHAR(1),
    dw_ld_ts CHAR(22)
  ) ;

  UPDATE conf.trgx_cocacola SET dw_in_use = '0' WHERE dw_in_use = '1' AND key =  'KPI' ;
  INSERT INTO conf.trgx_cocacola (
    key,
    data,
    dw_in_use,
    dw_ld_ts
  ) VALUES (
    'KPI', --> key
    '
{"全体-Total / 所有渠道"
 {:DATA {:c_sort 1 :c_total_score 100 :c_weight 0.10}
  :BRANCH {:TOTAL
           {"全体-Total / 所有渠道"
            {:DATA {:c_sort 1 :c_total_score 100 :c_weight 0.10}
             :CHILDREN { "全体-HMKT / 大卖场"
                         {:DATA {:c_sort 1 :c_total_score 100 :c_weight 0.10} :CHILDREN {}}
                         "全体-SMKT / 超市",
                         {:DATA {:c_sort 2 :c_total_score 100 :c_weight 0.20} :CHILDREN {}}
                         "全体-GT / 传统食杂"
                         {:DATA {:c_sort 3 :c_total_score 100 :c_weight 0.55} :CHILDREN {}}
                         "全体-E&D M/H / 中高档餐饮"
                         {:DATA {:c_sort 4 :c_total_score 100 :c_weight 0.08} :CHILDREN {}}
                         "全体-E&D Trad / 传统餐饮"
                         {:DATA {:c_sort 5 :c_total_score 100 :c_weight 0.07} :CHILDREN {}}
                         "产品铺货率-Total / 所有渠道"
                         {:DATA {:c_sort 1 :c_total_score 29.5 :c_weight 0.295} :CHILDREN {}}
                         "排面占有率-Total / 所有渠道"
                         {:DATA {:c_sort 2 :c_total_score 27.5 :c_weight 0.275} :CHILDREN {}}
                         "冰柜-Total / 所有渠道"
                         {:DATA {:c_sort 3 :c_total_score 24.9 :c_weight 0.249} :CHILDREN {}}
                         "渠道执行-Total / 所有渠道"
                         {:DATA {:c_sort 4 :c_total_score 12.75 :c_weight 0.1275} :CHILDREN {}}
                         "价格沟通-Total / 所有渠道"
                         {:DATA {:c_sort 5 :c_total_score 5.35 :c_weight 0.0535} :CHILDREN {}} }}}
           :CODE
           {"[G22]HMKT / 大卖场"
            {:DATA {:c_sort 1 :c_total_score 100 :c_weight 0.10}
             :CHILDREN {"[G23]Availability / 产品铺货"
                        {:DATA {:c_sort 2 :c_total_score 20 :c_weight 0.02}
                         :CATEGORY "产品铺货"
                         :CHILDREN {"[G24]Sparkling / 汽水"
                                    {:DATA {:c_sort 3 :c_total_score 13 :c_weight 0.013}
                                     :CHILDREN {"[H8]330ml CAN/Sleek CAN (Coke & Sprite & Fanta Orange & Schweppes +C)"
                                                {:DATA {:c_sort 4 :c_total_score 3 :c_weight 0.003 :abbreviation "Availability of Sparkling 330ml Can in H/S"} :CHILDREN {}}
                                                "[H9]500/600ml PET (Coke & Sprite)"
                                                {:DATA {:c_sort 5 :c_total_score 2 :c_weight 0.002 :abbreviation "Availability of Coke&Sprite 500/600ml PET in H/S"} :CHILDREN {}}
                                                "[H17]500/600ml PET (Fanta Orange & Fanta any other 2 flavors & Schweppes +C)"
                                                {:DATA {:c_sort 6 :c_total_score 2 :c_weight 0.002 :abbreviation "Availability of Fanta&+C  500/600ml PET in H/S"} :CHILDREN {}}
                                                "[H10]888ml/1L/1.25L/1.5L PET (Coke & Sprite & Fanta Orange)"
                                                {:DATA {:c_sort 7 :c_total_score 2 :c_weight 0.002 :abbreviation "Availability of Sparkling 888ML/1.25L/1.5L in H/S"} :CHILDREN {}}
                                                "[H11]2L/2L+ PET (Coke & Sprite & Fanta Orange)"
                                                {:DATA {:c_sort 8 :c_total_score 2 :c_weight 0.002 :abbreviation "Availability of Sparkling 2L/2L+ in H/S"} :CHILDREN {}}
                                                "[H12]Multi-Pack  4/6/8 CAN including Sleek CAN (Coke & Sprite)"
                                                {:DATA {:c_sort 9 :c_total_score 2 :c_weight 0.002 :abbreviation "Availability of Sparkling Multi-pack Can in H/S"} :CHILDREN {}} }}
                                    "[G25]STILL / 非汽水"
                                    {:DATA {:c_sort 10 :c_total_score 7 :c_weight 0.007}
                                     :CHILDREN {"[H13]450ml PET (MMPO & MM any other 4 flavors)"
                                                {:DATA {:c_sort 11 :c_total_score 4 :c_weight 0.004 :abbreviation "Availability of MM 450ml PET in H/S"} :CHILDREN {}}
                                                "[H14]MMPO MS PET 1.25L+1.8L"
                                                {:DATA {:c_sort 12 :c_total_score 1 :c_weight 0.001 :abbreviation "Availability of MMPO MS PET in H/S"} :CHILDREN {}}
                                                "[H15]550ml PET Icedew"
                                                {:DATA {:c_sort 13 :c_total_score 1 :c_weight 0.001 :abbreviation "Availability of Icedrew in H/S"} :CHILDREN {}}
                                                "[H16]450ml PET Dairy (any 4 flavors)"
                                                {:DATA {:c_sort 14 :c_total_score 1 :c_weight 0.001 :abbreviation "Availability of Dairy 450ml in H/S"} :CHILDREN {}} }}}}
                        "[G26]SOVI / 排面占有率"
                        {:DATA {:c_sort 15 :c_total_score 40 :c_weight 0.04}
                         :CATEGORY "排面占有率"
                         :CHILDREN {"[H1]KO NARTD share of shelf&cooler achieved 50%"
                                    {:DATA {:c_sort 16 :c_total_score 10 :c_weight 0.01 :abbreviation "KO NARTD SOVI in H/S"} :CHILDREN {}}
                                    "[H2]KO sparkling share of shelf&cooler achieved 80%"
                                    {:DATA {:c_sort 17 :c_total_score 15 :c_weight 0.015 :abbreviation "KO Sparkling SOVI in H/S"} :CHILDREN {}}
                                    "[H3]KO juice share of shelf&cooler achieved 50%"
                                    {:DATA {:c_sort 18 :c_total_score 15 :c_weight 0.015 :abbreviation "KO Juice SOVI in H/S"} :CHILDREN {}} }}
                        "[G27]Cooler / 冰柜"
                        {:DATA {:c_sort 19 :c_total_score 10 :c_weight 0.01}
                         :CATEGORY "冰柜"
                         :CHILDREN {"[H4]Purity - 95% KO"
                                    {:DATA {:c_sort 20 :c_total_score 5 :c_weight 0.005 :abbreviation "Cooler Purity in H/S"} :CHILDREN {}}
                                    "[H5]The ratio of No. of KO door vs KO+KB achieved 80%"
                                    {:DATA {:c_sort 21 :c_total_score 5 :c_weight 0.005 :abbreviation "Cooler Door in H/S"} :CHILDREN {}}}}
                        "[H6]Activation - 2nd display sqm of KO/KO+KB achieved 80%"
                        {:DATA {:c_sort 22 :c_total_score 25 :c_weight 0.025 :abbreviation "Activation - 2nd display in H/S"} :CATEGORY "渠道活动" :CHILDREN {}}
                        "[H7]Price Communication - KO SKU Price label"
                        {:DATA {:c_sort 23 :c_total_score 5 :c_weight 0.005 :abbreviation "Price Comm. in H/S"} :CATEGORY "价格沟通" :CHILDREN {}} }}
            "[G01]SMKT / 超市"
            {:DATA {:c_sort 24 :c_total_score 100 :c_weight 0.2}
             :CHILDREN {"[G02]Availability / 产品铺货"
                        {:DATA {:c_sort 25 :c_total_score 25 :c_weight 0.05}
                         :CATEGORY "产品铺货"
                         :CHILDREN {"[G03]Sparkling / 汽水"
                                   {:DATA {:c_sort 26 :c_total_score 16 :c_weight 0.032}
                                    :CHILDREN {"[F8]330ml CAN/Sleek CAN (Coke & Sprite & Fanta Orange & Schweppes +C)"
                                               {:DATA {:c_sort 27 :c_total_score 3.5 :c_weight 0.007 :abbreviation "Availability of Sparkling 330ml Can in H/S"} :CHILDREN {}}
                                               "[F9]500/600ml PET (Coke & Sprite)"
                                               {:DATA {:c_sort 28 :c_total_score 2.5 :c_weight 0.005 :abbreviation "Availability of Coke&Sprite 500/600ml PET in H/S"} :CHILDREN {}}
                                               "[N1]500/600ml PET (Fanta Orange & Fanta any other flavor & Schweppes +C)"
                                               {:DATA {:c_sort 29 :c_total_score 2.5 :c_weight 0.005 :abbreviation "Availability of Fanta&+C  500/600ml PET in H/S"} :CHILDREN {}}
                                               "[F10]888ml/1L/1.25L/1.5L PET (Coke & Sprite & Fanta Orange)"
                                               {:DATA {:c_sort 30 :c_total_score 2.5 :c_weight 0.005 :abbreviation "Availability of Sparkling 888ML/1.25L/1.5L in H/S"} :CHILDREN {}}
                                               "[F11]2L/2L+ PET (Coke & Sprite & Fanta Orange)"
                                               {:DATA {:c_sort 31 :c_total_score 2.5 :c_weight 0.005 :abbreviation "Availability of Sparkling 2L/2L+ in H/S"} :CHILDREN {}}
                                               "[F12]Multi-Pack  4/6/8 CAN including Sleek CAN (Coke & Sprite)"
                                               {:DATA {:c_sort 32 :c_total_score 2.5 :c_weight 0.005 :abbreviation "Availability of Sparkling Multi-pack Can in H/S"} :CHILDREN {}} }}
                                   "[G04]STILL / 非汽水"
                                   {:DATA {:c_sort 33 :c_total_score 9 :c_weight 0.018}
                                    :CHILDREN {"[F13]450ml PET (MMPO & MM any other 3 flavors)"
                                               {:DATA {:c_sort 34 :c_total_score 4.5 :c_weight 0.009 :abbreviation "Availability of MM 450ml PET in H/S"} :CHILDREN {}}
                                               "[F14]MMPO MS PET 1.25L/1.8L"
                                               {:DATA {:c_sort 35 :c_total_score 1.5 :c_weight 0.003 :abbreviation "Availability of MMPO MS PET in H/S"} :CHILDREN {}}
                                               "[F15]550ml PET Icedew"
                                               {:DATA {:c_sort 36 :c_total_score 1.5 :c_weight 0.003 :abbreviation "Availability of Icedrew in H/S"} :CHILDREN {}}
                                               "[F16]450ml PET Dairy (any 2 flavors)"
                                               {:DATA {:c_sort 37 :c_total_score 1.5 :c_weight 0.003 :abbreviation "Availability of Dairy 450ml in H/S"} :CHILDREN {}} }} }}
                        "[G05]SOVI / 排面占有率"
                        {:DATA {:c_sort 38 :c_total_score 35 :c_weight 0.07}
                         :CATEGORY "排面占有率"
                         :CHILDREN {"[F1]KO NARTD share of shelf&cooler achieved 50%"
                                    {:DATA {:c_sort 39 :c_total_score 9 :c_weight 0.018 :abbreviation "KO NARTD SOVI in H/S"} :CHILDREN {}}
                                    "[F2]KO sparkling share of shelf&cooler achieved 80%"
                                    {:DATA {:c_sort 40 :c_total_score 13 :c_weight 0.026 :abbreviation "KO Sparkling SOVI in H/S"} :CHILDREN {}}
                                    "[F3]KO juice share of shelf&cooler achieved 50%"
                                    {:DATA {:c_sort 41 :c_total_score 13 :c_weight 0.026 :abbreviation "KO Juice SOVI in H/S"} :CHILDREN {}} }}
                        "[G06]Cooler / 冰柜"
                        {:DATA {:c_sort 42 :c_total_score 20 :c_weight 0.04}
                         :CATEGORY "冰柜"
                         :CHILDREN {"[F4]Purity - 95% KO"
                                    {:DATA {:c_sort 43 :c_total_score 10 :c_weight 0.02 :abbreviation "Cooler Purity in H/S"} :CHILDREN {}}
                                    "[F5]The ratio of No. of KO door vs KO+KB achieved 80%"
                                    {:DATA {:c_sort 44 :c_total_score 10 :c_weight 0.02 :abbreviation "Cooler Door in H/S"} :CHILDREN {}} }}
                        "[F6]Activation - 2nd display sqm of KO/KO+KB achieved 80%" 
                        {:DATA {:c_sort 45 :c_total_score 15 :c_weight 0.03 :abbreviation "Activation - 2nd display in H/S"} :CATEGORY "渠道活动" :CHILDREN {}}
                        "[F7]Price Communication - KO SKU Price label"
                        {:DATA {:c_sort 46 :c_total_score 5 :c_weight 0.01 :abbreviation "Price Comm. in H/S"} :CATEGORY "价格沟通" :CHILDREN {}} }}
            "[G07]GT 传统食杂"
            {:DATA {:c_sort 47 :c_total_score 100 :c_weight 0.55}
             :CHILDREN {"[G08]Availability / 产品铺货"
                        {:DATA {:c_sort 48 :c_total_score 30 :c_weight 0.165}
                         :CATEGORY "产品铺货"
                         :CHILDREN {"[G09]Sparkling / 汽水"
                                    {:DATA {:c_sort 49 :c_total_score 16 :c_weight 0.088}
                                     :CHILDREN {"[F25]300ml PET (Coke & Sprite)"
                                                {:DATA {:c_sort 50 :c_total_score 3 :c_weight 0.0165 :abbreviation "Availability of Sparkling 300ml PET  in GT"} :CHILDREN {}}
                                                "[F26]330ml CAN (Coke & Sprite)"
                                                {:DATA {:c_sort 51 :c_total_score 3 :c_weight 0.0165 :abbreviation "Availability of Sparkling 330ml CAN in GT"} :CHILDREN {}}
                                                "[F27]500/600ml PET (Coke & Sprite & Fanta Orange & Schweppes +C)"
                                                {:DATA {:c_sort 52 :c_total_score 7 :c_weight 0.0385 :abbreviation "Availability of Sparkling 500/600ml PET in GT"} :CHILDREN {}}
                                                "[F28]1L/1.25L/1.5L/2L PET (Coke & Sprite)"
                                                {:DATA {:c_sort 53 :c_total_score 3 :c_weight 0.0165 :abbreviation "Availability of Sparkling 1.25L/1.5L/2L PET in GT"} :CHILDREN {}} }}
                                    "[G10]STILL / 非汽水"
                                    {:DATA {:c_sort 54 :c_total_score 14 :c_weight 0.077}
                                     :CHILDREN {"[F29]450ml PET (MMPO & MM any other 2 flavors)"
                                                {:DATA {:c_sort 55 :c_total_score 8 :c_weight 0.044 :abbreviation "Availability of MM 450ml PET in GT"} :CHILDREN {}}
                                                "[F30]MMPO MS PET 1.25L/1.8L"
                                                {:DATA {:c_sort 56 :c_total_score 3 :c_weight 0.0165 :abbreviation "Availability of MMPO MS PET in GT"} :CHILDREN {}}
                                                "[F31]550ml PET Icedew"
                                                {:DATA {:c_sort 57 :c_total_score 1 :c_weight 0.0055 :abbreviation "Availability of Icedrew in GT"} :CHILDREN {}}
                                                "[F32]450ml PET Dairy (any flavor)"
                                                {:DATA {:c_sort 58 :c_total_score 2 :c_weight 0.011 :abbreviation "Availability of Dairy 450ml in GT"} :CHILDREN {}} }}}}
                        "[G11]SOVI / 排面占有率"
                        {:DATA {:c_sort 59 :c_total_score 30 :c_weight 0.165}
                         :CATEGORY "排面占有率"
                         :CHILDREN {"[F17]KO NARTD share of shelf&cooler achieved 50%"
                                    {:DATA {:c_sort 60 :c_total_score 8 :c_weight 0.044 :abbreviation "KO NARTD SOVI in GT"} :CHILDREN {}}
                                    "[F18]KO sparkling share of shelf&cooler achieved 80%"
                                    {:DATA {:c_sort 61 :c_total_score 11 :c_weight 0.0605 :abbreviation "KO Sparkling SOVI in GT"} :CHILDREN {}}
                                    "[F19]KO juice share of shelf&cooler achieved 50%"
                                    {:DATA {:c_sort 62 :c_total_score 11 :c_weight 0.0605 :abbreviation "KO Juice SOVI in GT"} :CHILDREN {}} }}
                        "[G12]Cooler / 冰柜"
                        {:DATA {:c_sort 63 :c_total_score 30 :c_weight 0.165}
                         :CATEGORY "冰柜"
                         :CHILDREN {"[F20]Purity"
                                    {:DATA {:c_sort 64 :c_total_score 17 :c_weight 0.0935 :abbreviation "Cooler Purity in GT"} :CHILDREN {}}
                                    "[F21]1st Position"
                                    {:DATA {:c_sort 65 :c_total_score 5 :c_weight 0.0275 :abbreviation "Cooler 1st Position in GT"} :CHILDREN {}}
                                    "[F22]Penetration Rate"
                                    {:DATA {:c_sort 66 :c_total_score 8 :c_weight 0.044 :abbreviation "Cooler Penetration in GT"} :CHILDREN {}} }}
                        "[F23]Activation - KO product available,2nd Display with POSM"
                        {:DATA {:c_sort 67 :c_total_score 5 :c_weight 0.0275 :abbreviation "Activation-2nd Display in GT"} :CATEGORY "渠道活动" :CHILDREN {}}
                        "[F24]Price Communication - Package label"
                        {:DATA {:c_sort 68 :c_total_score 5 :c_weight 0.0275 :abbreviation "Price Comm. in GT"} :CATEGORY "价格沟通" :CHILDREN {}} }}
            "[G13]E&D M/H / 中高档餐饮"
            {:DATA {:c_sort 69 :c_total_score 100 :c_weight 0.08}
             :CHILDREN {"[G14]Availability / 产品铺货"
                        {:DATA {:c_sort 70 :c_total_score 40 :c_weight 0.032}
                         :CATEGORY "产品铺货"
                         :CHILDREN {"[G15]Sparkling / 汽水"
                                    {:DATA {:c_sort 71 :c_total_score 25 :c_weight 0.02}
                                     :CHILDREN {"[F37]330ml CAN/Sleek CAN (Coke & Sprite)"
                                                {:DATA {:c_sort 72 :c_total_score 10 :c_weight 0.008 :abbreviation "Availability of Sparkling CAN in E&D M/H"} :CHILDREN {}}
                                                "[F38]MS PET 1L/1.25L/1.5L/2L/2L+ (Coke & Sprite)"
                                                {:DATA {:c_sort 73 :c_total_score 15 :c_weight 0.012 :abbreviation "Availability of Sparkling MS in E&D M/H"} :CHILDREN {}} }}
                                    "[G16]STILL / 非汽水"
                                    {:DATA {:c_sort 74 :c_total_score 15 :c_weight 0.012}
                                     :CHILDREN {"[F39]MMPO MS PET 1.25L/1.8L"
                                                {:DATA {:c_sort 75 :c_total_score 15 :c_weight 0.012 :abbreviation "Availability of MMPO MS in E&D M/H"} :CHILDREN {}} }}}}
                        "[F33]Cooler - 40% KO Inventory rate"
                        {:DATA {:c_sort 76 :c_total_score 25 :c_weight 0.02 :abbreviation "Cooler Inventory in E&D M/H"} :CATEGORY "冰柜" :CHILDREN {}}
                        "[G17]Activation / 渠道活动"
                        {:DATA {:c_sort 77 :c_total_score 30 :c_weight 0.024}
                         :CATEGORY "渠道活动"
                         :CHILDREN {"[F34]Merchandising Display"
                                    {:DATA {:c_sort 78 :c_total_score 15 :c_weight 0.012 :abbreviation "Merchandising in E&D M/H"} :CHILDREN {}}
                                    "[F35]Full Case Display or Bar Table Display"
                                    {:DATA {:c_sort 79 :c_total_score 15 :c_weight 0.012 :abbreviation "Display in E&D M/H"} :CHILDREN {}} }}
                        "[F36]Price Communication - Price label"
                        {:DATA {:c_sort 80 :c_total_score 5 :c_weight 0.004 :abbreviation "Price Comm. in E&D M/H"} :CATEGORY "价格沟通" :CHILDREN {}} }}
            "[G18]E&D Trad / 传统餐饮"
            {:DATA {:c_sort 81 :c_total_score 100 :c_weight 0.07}
             :CHILDREN {"[G19]Availability / 产品铺货"
                        {:DATA {:c_sort 82 :c_total_score 40 :c_weight 0.028}
                         :CATEGORY "产品铺货"
                         :CHILDREN {"[G20]Sparkling / 汽水"
                                    {:DATA {:c_sort 83 :c_total_score 40 :c_weight 0.028}
                                     :CHILDREN {"[F44]Price Communication - Price label"
                                                {:DATA {:c_sort 84 :c_total_score 40 :c_weight 0.028 :abbreviation "Availability of Sparkling RB in E&D Traditional"} :CHILDREN {}} }} }}
                        "[F40]Cooler - 40% KO Inventory rate"
                        {:DATA {:c_sort 85 :c_total_score 20 :c_weight 0.014 :abbreviation "Cooler Inventory in E&D Traditional"} :CATEGORY "冰柜" :CHILDREN {}}
                        "[G21]Activation / 渠道活动"
                        {:DATA {:c_sort 86 :c_total_score 30 :c_weight 0.021}
                         :CATEGORY "渠道活动"
                         :CHILDREN {"[F41]RB Combo Promotion"
                                    {:DATA {:c_sort 87 :c_total_score 20 :c_weight 0.014 :abbreviation "RB Combo Promotion in E&D Traditional"} :CHILDREN {}}
                                    "[F42]RB Full Case Display"
                                    {:DATA {:c_sort 88 :c_total_score 10 :c_weight 0.007 :abbreviation "RB Full Case Display in E&D Traditional"} :CHILDREN {}} }}
                        "[F43]Price Communication - KO RB price label"
                        {:DATA {:c_sort 89 :c_total_score 10 :c_weight 0.007 :abbreviation "Price Comm. in E&D Traditional"} :CATEGORY "价格沟通" :CHILDREN {}} }} } }}}
    ', --> data
    '1', --> dw_in_use
    TO_CHAR(CURRENT_TIMESTAMP(0), 'YYYY-MM-DD"T"HH24:MI:SSOF') --> dw_ld_ts
  ) ;
  
