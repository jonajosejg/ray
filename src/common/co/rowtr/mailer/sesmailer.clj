(ns co.rowtr.mailer.sesmailer
  (:require
    [co.rowtr.storage               :as st]
    [co.rowtr.mailer                :as mail  :refer [IMail check-state html! text!]]
    [amazonica.aws.simpleemail      :as ses]
    [selmer.parser                  :as sel]))

(defn- send-mail [{:keys [from to subject text html] :as mail}]
  (ses/send-email :destination  {:to-addresses [to]}
                  :source       from
                  :message      {:subject subject :body {:html html :text text}}))

(defrecord MailMachine
  [html text html-file text-file mail-type from to data state]


  IMail
  (check-state [this]
    (let [{:keys [from to html text]} this]
      (assoc this :state (if (and from to html text) :ready :not-ready))))
  (state? [this] (:state this))
  (html! [this]
    (let [html    (when (and (:data this) (:html-file this)) (sel/render (:html-file this) (:data this)))]
      (check-state (assoc this :html html))))
  (text! [this]
    (let [text    (when (and (:data this) (:text-file this)) (sel/render (:text-file this) (:data this)))]
      (check-state (assoc this :text text))))
  (data! [this data]
    (when (map? data (check-state (assoc this :data data)))))
  (mail! [this]
    (if-not (= (:state this) :ready)
      (cond
        (not (:to this))        (throw (ex-info "Mailer Error" {:cause "No Recipient Specified"}))
        (not (:html this))      (throw (ex-info "Mailer Error" {:cause "No HTML Version"}))
        (not (:text this))      (throw (ex-info "Mailer Error" {:cause "No Text Version"}))
        :else                   (throw (ex-info "Mailer Error" {})))
      (let [resp        (send-mail this)
            logger      (:logger this)]
        (when logger (logger {:id (:message-id resp) :recipient (:to this) :template (:mail-type this) :data (:data this)}))))))



(defn mail-machine [& {:keys [mail-type from to subject data logger storage-provider] :as args}]
  (let [storage                 (or storage-provider (throw (ex-info "Mailer Error" {:cause "Storage Provider Missing"})))
        templates               (st/fetch-email-templates storage mail-type)
        this                    {:mail-type   mail-type
                                 :html        nil
                                 :text        nil
                                 :to          to
                                 :from        from
                                 :subject     subject
                                 :data        data
                                 :logger      logger
                                 :html-file   (:html templates)
                                 :text-file   (:text templates)
                                 :state       :not-ready}
        this    (map->MailMachine this)
        this    (html! this)
        this    (text! this)]
    this))
