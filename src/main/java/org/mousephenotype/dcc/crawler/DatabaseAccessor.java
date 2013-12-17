/*
 * Copyright 2012 Medical Research Council Harwell.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.mousephenotype.dcc.crawler;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.persistence.*;
import org.mousephenotype.dcc.crawler.entities.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXParseException;

/**
 * An instance of the singleton class DatabaseAccessor provides access to the
 * underlying persistence context.
 *
 * For the moment, we use coarse-grained (function-based) synchronisation. This
 * may be improved further by using context-sensitive locks, thus separating out
 * the related accesses into synchronisation groups. This will allow unrelated
 * access to the database interleave between synchronisation groups, thus
 * improving performance.
 *
 * @author Gagarine Yaikhom <g.yaikhom@har.mrc.ac.uk>
 */
public class DatabaseAccessor {

    private final Logger logger = LoggerFactory.getLogger(DatabaseAccessor.class);
    private static final String SHORT_NAME = "shortName";
    private static final String ZIP_ACTION_ID = "zaId";
    private static final String PHASE_ID = "phaseId";
    private static final String STATUS_ID = "statusId";
    private static final String PERSISTENCE_UNIT_NAME =
            "org.mousephenotype.dcc.crawler.entities.pu";
    private static DatabaseAccessor instance = null;
    private EntityManagerFactory emf;
    // Five attempts to connect with the database, after waiting for
    // 5 mins, 25 mins, 125 mins (2.0833 hrs),
    // 625 mins (10.5167 hrs), and 3125 minutes (2.17014 days)
    // After these many attempts, the application will exit as failure.
    private static final int MAX_DATABASE_CONNECTION_ATTEMPTS = 5;
    private static final int MINUTES_TO_WAIT_BEFORE_CONNECTION_ATTEMPT = 5;
    private static final int MINUTES_TO_MILLISECONDS = 60000;

    private void exit(int code) {
        logger.error("Application will now exit!");
        System.exit(code);
    }

    private Map<String, String> getDatabaseProperties() {
        SettingsManager sm = SettingsManager.getInstance();
        Map<String, String> props = new HashMap<>();
        if (sm.hasCustomCrawlerSettings()) {
            props.put("javax.persistence.jdbc.driver", sm.getDriver());
            props.put("javax.persistence.jdbc.url", sm.getTrackerUrl());
            props.put("javax.persistence.jdbc.user", sm.getTrackerUser());
            props.put("javax.persistence.jdbc.password", sm.getTrackerPassword());
        }
        return props;
    }

    private void createEntityManagerFactory() {
        emf = null;
        try {
            emf = Persistence.createEntityManagerFactory(PERSISTENCE_UNIT_NAME,
                    getDatabaseProperties());
        } catch (Exception e) {
            logger.error("Failed to initialise persistence unit for database");
            logger.error("Check persistence.xml phenodcc-crawler-entities.");
            exit(1);
        }
    }

    protected DatabaseAccessor() {
        createEntityManagerFactory();
    }

    public static synchronized DatabaseAccessor getInstance() {
        if (instance == null) {
            instance = new DatabaseAccessor();
        }
        return instance;
    }

    public synchronized void closeEntityManagerFactory() {
        if (emf != null && emf.isOpen()) {
            emf.close();
        }
    }

    public synchronized EntityManagerFactory getEntityManagerFactory() {
        if (emf == null || !emf.isOpen()) {
            logger.error("Invalid database persistence unit.");
            logger.error("EntityManagerFactory is undefined!");
            exit(1);
        }
        return emf;
    }

    private synchronized EntityManager createEntityManager() {
        EntityManager em = null;
        int attempt = 1;
        long wait = MINUTES_TO_WAIT_BEFORE_CONNECTION_ATTEMPT;

        while (attempt <= MAX_DATABASE_CONNECTION_ATTEMPTS) {
            em = null;
            try {
                em = getEntityManagerFactory().createEntityManager();
            } catch (Exception e) {
                logger.warn("Failed to establish connection with database");
                logger.warn("Unable to create entity manager in attempt {}",
                        attempt);
            }
            if (em == null) {
                try {
                    logger.info("Will wait {} minutes before next attempt",
                            wait);
                    Thread.sleep(MINUTES_TO_MILLISECONDS * wait);
                } catch (InterruptedException e) {
                    logger.warn("Woken up while sleeping before retry");
                }
                /* exponential back-off before next attempt */
                wait *= MINUTES_TO_WAIT_BEFORE_CONNECTION_ATTEMPT;
                ++attempt;
            } else {
                if (attempt > 1) {
                    logger.info("Managed to establish connection with "
                            + "database in attempt {}", attempt);
                }
                break;
            }
        }

        if (em == null) {
            logger.error("Could not establish connection with database server");
            logger.error("Application will now exit!");
            System.exit(1);
        }

        return em;
    }

    private synchronized AnException createAnException(EntityManager em,
            String n) {
        AnException returnValue = null;
        try {
            em.getTransaction().begin();
            AnException ae = new AnException(n);
            em.persist(ae);
            em.getTransaction().commit();
            em.refresh(ae);
            returnValue = ae;
        } catch (EntityExistsException |
                IllegalArgumentException |
                TransactionRequiredException e) {
            logger.error(e.getMessage());
        }
        return returnValue;
    }

    private synchronized AnException getAnException(EntityManager em,
            String n) {
        AnException returnValue = null;
        try {
            TypedQuery<AnException> q = em.createNamedQuery(
                    "AnException.findByShortName", AnException.class);
            q.setParameter(SHORT_NAME, n);
            returnValue = q.getSingleResult();
        } catch (NoResultException e) {
            logger.warn("There is no exception named '{}'; will create one", n);
            returnValue = createAnException(em, n);
        } catch (NonUniqueResultException e) {
            logger.error(e.getMessage());
            exitCorruptDatabase();
        }
        return returnValue;
    }

    public synchronized void addXmlErrorLogs(XmlFile xf,
            List<SAXParseException> issues) {
        EntityManager em = createEntityManager();
        try {
            AnException ae = getAnException(em, "SAXParseException");
            em.getTransaction().begin();
            for (SAXParseException ex : issues) {
                XmlLog xl = new XmlLog(xf, ae, ex.getMessage(),
                        ex.getLineNumber(), ex.getColumnNumber());
                em.persist(xl);
                em.flush();
                em.refresh(xl);
            }
            em.getTransaction().commit();
        } catch (EntityExistsException |
                IllegalArgumentException |
                TransactionRequiredException e) {
            logger.error(e.getMessage());
        } finally {
            if (em.isOpen()) {
                em.close();
            }
        }
    }

    public synchronized void addXmlErrorLog(XmlFile xf, String issue) {
        EntityManager em = createEntityManager();
        try {
            AnException ae = getAnException(em, "DataInsertionException");
            em.getTransaction().begin();
            XmlLog xl = new XmlLog(xf, ae, issue);
            em.persist(xl);
            em.getTransaction().commit();
            em.refresh(xl);
        } catch (EntityExistsException |
                IllegalArgumentException |
                TransactionRequiredException e) {
            logger.error(e.getMessage());
        } finally {
            if (em.isOpen()) {
                em.close();
            }
        }
    }

    public synchronized void addXmlErrorLog(XmlFile xf, SAXParseException ex) {
        EntityManager em = createEntityManager();
        try {
            AnException ae = getAnException(em, "DataInsertionException");
            em.getTransaction().begin();
            XmlLog xl = new XmlLog(xf, ae, ex.getMessage(), ex.getLineNumber(),
                    ex.getColumnNumber());
            em.persist(xl);
            em.getTransaction().commit();
            em.refresh(xl);
        } catch (EntityExistsException |
                IllegalArgumentException |
                TransactionRequiredException e) {
            logger.error(e.getMessage());
        } finally {
            if (em.isOpen()) {
                em.close();
            }
        }
    }

    public synchronized void addZipErrorLog(ZipDownload zd, Exception ex) {
        EntityManager em = createEntityManager();
        try {
            AnException ae = getAnException(em, ex.getClass().getName());
            em.getTransaction().begin();
            ZipLog zl = new ZipLog(zd, ae, ex.getMessage());
            em.persist(zl);
            em.getTransaction().commit();
            em.refresh(zl);
        } catch (EntityExistsException |
                IllegalArgumentException |
                TransactionRequiredException e) {
            logger.error(e.getMessage());
        } finally {
            if (em.isOpen()) {
                em.close();
            }
        }
    }
    // The unique identifiers for the phase and status are ordered according
    // to their temporal sequence for the phase, and severity sequence for the
    // status. Hence, when the phase and status is updated, we must ensure that
    // the following post conditions hold after an update:
    //
    // 1. The identifier of the new phase must not be less than the identifer
    //    of the current phase. This ensures that processing only moves forward
    //    in the processing pipeline (download, validate, insert, and so on).
    //
    // 2. If the phase identifiers are the same, the identifier of the new
    //    status must not be less than the identifier of the current status.
    //    This ensures that the severity of any existing issue is not
    //    overridden by less severe issues.
    //
    // The only exception is when an issue ('failed' status) is being escalated
    // from the successor phases; in which case, the phase and status must be
    // set to satisfy the reverse of the above post conditions. That is, the
    // identifiers for the phase must be less than the existing values to be
    // effective. If it is in the same phase, the lower status should prevail.
    //
    // For instance, if the first XML document was extracted successfully but
    // did not pass XML validation, the phase and status for that XML document
    // will be set to ('xml', 'failed'). This will also be escalated to its
    // predecessors, i.e., zip_download, and zip_action. Now, if we were
    // processing the next XML document, and it failed at data integrity check
    // (which is the phase after 'xml'), we will set ('data', 'failed') for
    // this XML document; however, when we escalate the problem, it will be
    // ignored to preserve the fact that XML validation failed for one of the
    // documents. In this manner, we preserve the earliest failure point.
    private static final int IGNORE_PHASE_STATUS = 0;
    private static final int IGNORE_PHASE = 1;
    private static final int IGNORE_STATUS = 2;
    private static final int IGNORE_NONE = 3;

    private int checkPhaseStatus(Phase phaseNew, Phase phaseOld,
            AStatus statusNew, AStatus statusOld) {
        int returnValue;
        boolean isAlreadyErroneous = AStatus.FAILED.equals(statusOld.getShortName());
        boolean isNewError = AStatus.FAILED.equals(statusNew.getShortName());
        if (isAlreadyErroneous || isNewError) {
            if (isAlreadyErroneous) {
                if (isNewError) {
                    if (phaseNew.getId() < phaseOld.getId()) {
                        returnValue = IGNORE_STATUS; // overwrite 'earliest failure' phase
                    } else {
                        returnValue = IGNORE_PHASE_STATUS; // preserve 'earliest failure'
                    }
                } else {
                    returnValue = IGNORE_PHASE_STATUS; // preserve 'earliest failure'
                }
            } else {
                returnValue = IGNORE_NONE; // record first failure
            }
        } else {
            // There has never been an error, and neither is the new status
            int t = phaseNew.getId() - phaseOld.getId();
            if (t < 0) {
                returnValue = IGNORE_PHASE_STATUS;
            } else {
                if (t == 0) {
                    if (statusNew.getId() > statusOld.getId()) {
                        returnValue = IGNORE_PHASE;
                    } else {
                        returnValue = IGNORE_PHASE_STATUS;
                    }
                } else {
                    returnValue = IGNORE_NONE;
                }
            }
        }
        return returnValue;
    }

    private synchronized void setZipActionPhaseStatus(EntityManager em, ZipAction za, Phase p, AStatus s) {
        int t = checkPhaseStatus(p, za.getPhaseId(), s, za.getStatusId());
        if (t == IGNORE_PHASE_STATUS) {
            return;
        }
        em.getTransaction().begin();
        if (t != IGNORE_PHASE) {
            za.setPhaseId(p);
        }
        if (t != IGNORE_STATUS) {
            za.setStatusId(s);
        }
        em.getTransaction().commit();
    }

    private synchronized void setZipDownloadPhaseStatus(EntityManager em, ZipDownload zd, Phase p, AStatus s) {
        int t = checkPhaseStatus(p, zd.getPhaseId(), s, zd.getStatusId());
        if (t == IGNORE_PHASE_STATUS) {
            return;
        }
        em.getTransaction().begin();
        if (t != IGNORE_PHASE) {
            zd.setPhaseId(p);
        }
        if (t != IGNORE_STATUS) {
            zd.setStatusId(s);
        }
        em.getTransaction().commit();
    }

    private synchronized void setXmlFilePhaseStatus(EntityManager em, XmlFile xf, Phase p, AStatus s) {
        int t = checkPhaseStatus(p, xf.getPhaseId(), s, xf.getStatusId());
        if (t == IGNORE_PHASE_STATUS) {
            return;
        }
        em.getTransaction().begin();
        if (t != IGNORE_PHASE) {
            xf.setPhaseId(p);
        }
        if (t != IGNORE_STATUS) {
            xf.setStatusId(s);
        }
        em.getTransaction().commit();
    }

    public synchronized void setZipActionPhaseStatus(ZipAction zipAction, String phase, String status) {
        EntityManager em = createEntityManager();
        try {
            ZipAction za = em.find(ZipAction.class, zipAction.getId());
            if (za == null) {
                logger.warn("No zip action entity with id '{}'", zipAction.getId());
            } else {
                Phase p = getPhase(em, phase);
                AStatus s = getStatus(em, status);
                setZipActionPhaseStatus(em, za, p, s);
            }
        } catch (IllegalArgumentException | TransactionRequiredException e) {
            logger.error(e.getMessage());
        } finally {
            if (em.isOpen()) {
                em.close();
            }
        }
    }

    public synchronized void setZipDownloadPhaseStatus(ZipDownload zipDownload, String phase, String status) {
        EntityManager em = createEntityManager();
        try {
            ZipDownload zd = em.find(ZipDownload.class, zipDownload.getId());
            if (zd == null) {
                logger.warn("No zip download entity with id '{}'", zipDownload.getId());
                return;
            }
            Phase p = getPhase(em, phase);
            AStatus s = getStatus(em, status);
            setZipDownloadPhaseStatus(em, zd, p, s);
            setZipActionPhaseStatus(em, zd.getZfId().getZaId(), p, s);
        } catch (IllegalArgumentException | TransactionRequiredException e) {
            logger.error(e.getMessage());
        } finally {
            if (em.isOpen()) {
                em.close();
            }
        }
    }

    public synchronized void setDownloadProgress(ZipDownload zd, long byteCount) {
        EntityManager em = createEntityManager();
        try {
            ZipDownload zipDownload = em.find(ZipDownload.class, zd.getId());
            if (zipDownload != null) {
                em.getTransaction().begin();
                zipDownload.setReceived(new Date());
                zipDownload.setDownloadedSizeBytes(byteCount);
                em.getTransaction().commit();
            }
        } catch (IllegalArgumentException | TransactionRequiredException e) {
            logger.error(e.getMessage());
        } finally {
            if (em.isOpen()) {
                em.close();
            }
        }
    }

    public synchronized XmlFile setXmlFilePhaseStatus(XmlFile xmlFile, String phase, String status) {
        EntityManager em = createEntityManager();
        XmlFile xf = null;
        try {
            xf = em.find(XmlFile.class, xmlFile.getId());
            if (xf == null) {
                logger.warn("No XML file entity with id '{}'", xmlFile.getId());
            } else {
                Phase p = getPhase(em, phase);
                AStatus s = getStatus(em, status);
                setXmlFilePhaseStatus(em, xf, p, s);
                setZipDownloadPhaseStatus(em, xf.getZipId(), p, s);
                setZipActionPhaseStatus(em, xf.getZipId().getZfId().getZaId(), p, s);
                em.refresh(xf);
            }
        } catch (IllegalArgumentException | TransactionRequiredException e) {
            logger.error(e.getMessage());
        } finally {
            if (em.isOpen()) {
                em.close();
            }
        }
        return xf;
    }

    public synchronized ZipDownload downloadBegins(FileSourceHasZip f) {
        ZipDownload returnValue = null;
        EntityManager em = createEntityManager();
        try {
            Phase phase = getPhase(em, Phase.DOWNLOAD_FILE);
            AStatus status = getStatus(em, AStatus.RUNNING);
            FileSourceHasZip fileSourceHasZip = em.find(FileSourceHasZip.class, f.getId());
            if (fileSourceHasZip != null) {
                em.getTransaction().begin();
                ZipDownload zd = new ZipDownload(fileSourceHasZip, new Date(), new Date(), phase, status);
                em.persist(zd);
                em.getTransaction().commit();
                returnValue = zd;
            }
        } catch (EntityExistsException |
                IllegalArgumentException |
                TransactionRequiredException e) {
            logger.error(e.getMessage());
        } finally {
            if (em.isOpen()) {
                em.close();
            }
        }
        return returnValue;
    }

    public synchronized void downloadDone(ZipDownload zd) {
        EntityManager em = createEntityManager();
        try {
            AStatus status = getStatus(em, AStatus.DONE);
            ZipDownload zipDownload = em.find(ZipDownload.class, zd.getId());
            if (zipDownload != null) {
                em.getTransaction().begin();
                zipDownload.setReceived(new Date());
                zipDownload.setStatusId(status);
                zipDownload.getZfId().getZaId().setStatusId(status);
                em.getTransaction().commit();
            }
        } catch (IllegalArgumentException | TransactionRequiredException e) {
            logger.error(e.getMessage());
        } finally {
            if (em.isOpen()) {
                em.close();
            }
        }
    }

    public synchronized void downloadFailed(ZipDownload zd) {
        EntityManager em = createEntityManager();
        try {
            AStatus status = getStatus(em, AStatus.FAILED);
            ZipDownload zipDownload = em.find(ZipDownload.class, zd.getId());
            if (zipDownload != null) {
                em.getTransaction().begin();
                em.find(ZipDownload.class, zd.getId());
                zipDownload.setStatusId(status);
                zipDownload.getZfId().getZaId().setStatusId(status);
                em.getTransaction().commit();
            }
        } catch (IllegalArgumentException | TransactionRequiredException e) {
            logger.error(e.getMessage());
        } finally {
            if (em.isOpen()) {
                em.close();
            }
        }
    }

    public synchronized ZipFile getZipFile(String fn) {
        ZipFile returnValue = null;
        EntityManager em = createEntityManager();
        try {
            TypedQuery<ZipFile> q = em.createNamedQuery("ZipFile.findByFileName", ZipFile.class);
            q.setParameter("fileName", fn);
            returnValue = q.getSingleResult();
        } catch (NoResultException e) {
            logger.debug("There is no zip file with name '{}'", fn);
        } catch (NonUniqueResultException e) {
            logger.error(e.getMessage());
        } catch (IllegalStateException e) {
            logger.error(e.getMessage());
        } catch (QueryTimeoutException e) {
            logger.error(e.getMessage());
        } catch (TransactionRequiredException e) {
            logger.error(e.getMessage());
        } catch (PessimisticLockException e) {
            logger.error(e.getMessage());
        } catch (LockTimeoutException e) {
            logger.error(e.getMessage());
        } catch (PersistenceException e) {
            logger.error(e.getMessage());
        } finally {
            if (em.isOpen()) {
                em.close();
            }
        }
        return returnValue;
    }

    public synchronized ZipAction getZipAction(ZipFile zf, String todo) {
        ZipAction returnValue = null;
        EntityManager em = createEntityManager();
        try {
            ProcessingType pt = getProcessingType(todo);
            TypedQuery<ZipAction> q = em.createNamedQuery("ZipAction.findByZipAction", ZipAction.class);
            q.setParameter("zipId", zf);
            q.setParameter("todoId", pt);
            returnValue = q.getSingleResult();
        } catch (NoResultException e) {
            logger.debug("There is no zip action '{}' for the file named '{}'", todo, zf.getFileName());
        } catch (NonUniqueResultException e) {
            logger.error(e.getMessage());
        } catch (IllegalStateException e) {
            logger.error(e.getMessage());
        } catch (QueryTimeoutException e) {
            logger.error(e.getMessage());
        } catch (TransactionRequiredException e) {
            logger.error(e.getMessage());
        } catch (PessimisticLockException e) {
            logger.error(e.getMessage());
        } catch (LockTimeoutException e) {
            logger.error(e.getMessage());
        } catch (PersistenceException e) {
            logger.error(e.getMessage());
        } finally {
            if (em.isOpen()) {
                em.close();
            }
        }
        return returnValue;
    }

    public synchronized XmlFile getXmlFile(ZipDownload zd, String fn) {
        XmlFile returnValue = null;
        EntityManager em = createEntityManager();
        try {
            TypedQuery<XmlFile> q = em.createNamedQuery("XmlFile.findByZipFname", XmlFile.class);
            q.setParameter("zipId", zd);
            q.setParameter("fname", fn);
            returnValue = q.getSingleResult();
        } catch (NoResultException e) {
            logger.debug("There is no xml document named '{}'", fn);
        } catch (NonUniqueResultException e) {
            logger.error(e.getMessage());
        } catch (IllegalStateException e) {
            logger.error(e.getMessage());
        } catch (QueryTimeoutException e) {
            logger.error(e.getMessage());
        } catch (TransactionRequiredException e) {
            logger.error(e.getMessage());
        } catch (PessimisticLockException e) {
            logger.error(e.getMessage());
        } catch (LockTimeoutException e) {
            logger.error(e.getMessage());
        } catch (PersistenceException e) {
            logger.error(e.getMessage());
        } finally {
            if (em.isOpen()) {
                em.close();
            }
        }
        return returnValue;
    }

    public synchronized ZipAction getZipAction(Phase p, AStatus s) {
        ZipAction returnValue = null;
        EntityManager em = createEntityManager();
        try {
            TypedQuery<ZipAction> q = em.createNamedQuery("ZipAction.findByPhaseStatus", ZipAction.class);
            q.setParameter(PHASE_ID, p);
            q.setParameter(STATUS_ID, s);
            q.setFirstResult(0).setMaxResults(1);
            returnValue = q.getSingleResult();
        } catch (NoResultException e) {
            logger.debug("No zip action with phase '{}' and status '{}'", p.getShortName(), s.getShortName());
        } catch (NonUniqueResultException e) {
            logger.error(e.getMessage());
        } catch (IllegalStateException e) {
            logger.error(e.getMessage());
        } catch (QueryTimeoutException e) {
            logger.error(e.getMessage());
        } catch (TransactionRequiredException e) {
            logger.error(e.getMessage());
        } catch (PessimisticLockException e) {
            logger.error(e.getMessage());
        } catch (LockTimeoutException e) {
            logger.error(e.getMessage());
        } catch (PersistenceException e) {
            logger.error(e.getMessage());
        } finally {
            if (em.isOpen()) {
                em.close();
            }
        }
        return returnValue;
    }

    public synchronized ZipDownload getZipDownload(Phase p, AStatus s, ZipAction za) {
        ZipDownload returnValue = null;
        EntityManager em = createEntityManager();
        try {
            TypedQuery<ZipDownload> q = em.createNamedQuery("ZipDownload.findByZipAction", ZipDownload.class);
            q.setParameter(PHASE_ID, p);
            q.setParameter(STATUS_ID, s);
            q.setParameter(ZIP_ACTION_ID, za);
            q.setFirstResult(0).setMaxResults(1);
            returnValue = q.getSingleResult();
        } catch (NoResultException e) {
            logger.debug("No zip download for zip action '{}' with phase '{}' and status '{}'", new Object[]{za.getId(), p.getShortName(), s.getShortName()});
        } catch (NonUniqueResultException e) {
            logger.error(e.getMessage());
        } catch (IllegalStateException e) {
            logger.error(e.getMessage());
        } catch (QueryTimeoutException e) {
            logger.error(e.getMessage());
        } catch (TransactionRequiredException e) {
            logger.error(e.getMessage());
        } catch (PessimisticLockException e) {
            logger.error(e.getMessage());
        } catch (LockTimeoutException e) {
            logger.error(e.getMessage());
        } catch (PersistenceException e) {
            logger.error(e.getMessage());
        } finally {
            if (em.isOpen()) {
                em.close();
            }
        }
        return returnValue;
    }

    public synchronized List<ZipAction> getZipActionsAscCreated(Phase p, AStatus s) {
        List<ZipAction> returnValue = null;
        EntityManager em = createEntityManager();
        try {
            TypedQuery<ZipAction> q = em.createNamedQuery("ZipAction.findByPhaseStatusAscCreated", ZipAction.class);
            q.setParameter(PHASE_ID, p);
            q.setParameter(STATUS_ID, s);
            returnValue = q.getResultList();
        } catch (IllegalStateException |
                QueryTimeoutException |
                TransactionRequiredException |
                PessimisticLockException |
                LockTimeoutException e) {
            logger.error(e.getMessage());
        } catch (PersistenceException e) {
            logger.error(e.getMessage());
        } finally {
            if (em.isOpen()) {
                em.close();
            }
        }
        return returnValue;
    }

    public synchronized List<XmlFile> getXmlFiles(ZipDownload zd, String pattern) {
        List<XmlFile> returnValue = null;
        EntityManager em = createEntityManager();
        try {
            TypedQuery<XmlFile> q = em.createNamedQuery("XmlFile.findByZipDownload", XmlFile.class);
            q.setParameter("zipId", zd);
            q.setParameter("pattern", pattern);
            returnValue = q.getResultList();
        } catch (IllegalStateException |
                QueryTimeoutException |
                TransactionRequiredException |
                PessimisticLockException |
                LockTimeoutException e) {
            logger.error(e.getMessage());
        } catch (PersistenceException e) {
            logger.error(e.getMessage());
        } finally {
            if (em.isOpen()) {
                em.close();
            }
        }
        return returnValue;
    }

    public synchronized List<XmlFile> getXmlFilesByPhaseStatusTypeAscCreated(Phase p, AStatus s, String pattern) {
        List<XmlFile> returnValue = null;
        EntityManager em = createEntityManager();
        try {
            TypedQuery<XmlFile> q = em.createNamedQuery("XmlFile.findByPhaseStatusTypeAscCreated", XmlFile.class);
            q.setParameter(PHASE_ID, p);
            q.setParameter(STATUS_ID, s);
            q.setParameter("pattern", pattern);
            returnValue = q.getResultList();
        } catch (IllegalStateException |
                QueryTimeoutException |
                TransactionRequiredException |
                PessimisticLockException |
                LockTimeoutException e) {
            logger.error(e.getMessage());
        } catch (PersistenceException e) {
            logger.error(e.getMessage());
        } finally {
            if (em.isOpen()) {
                em.close();
            }
        }
        return returnValue;
    }

    public synchronized FileSource getFileSource(Centre centre, String hostname) {
        FileSource returnValue = null;
        EntityManager em = createEntityManager();
        try {
            ResourceState rs = getResourceState(FileSource.AVAILABLE);
            TypedQuery<FileSource> q = em.createNamedQuery("FileSource.findByCentreUrl", FileSource.class);
            q.setParameter("centreId", centre);
            q.setParameter("hostname", hostname);
            q.setParameter("stateId", rs);
            returnValue = q.getSingleResult();
        } catch (NoResultException e) {
            logger.debug("There is no file source at centre {} with hostname '{}'", centre.getShortName(), hostname);
        } catch (NonUniqueResultException e) {
            logger.error(e.getMessage());
        } catch (IllegalStateException |
                QueryTimeoutException |
                TransactionRequiredException |
                PessimisticLockException |
                LockTimeoutException e) {
            logger.error(e.getMessage());
        } catch (PersistenceException e) {
            logger.error(e.getMessage());
        } finally {
            if (em.isOpen()) {
                em.close();
            }
        }
        return returnValue;
    }

    public synchronized FileSourceHasZip getFileSourceHasZip(FileSource fileSource, ZipAction za) {
        FileSourceHasZip returnValue = null;
        EntityManager em = createEntityManager();
        try {
            TypedQuery<FileSourceHasZip> q = em.createNamedQuery("FileSourceHasZip.findBySourceAction", FileSourceHasZip.class);
            q.setParameter("fileSourceId", fileSource);
            q.setParameter(ZIP_ACTION_ID, za);
            returnValue = q.getSingleResult();
        } catch (NoResultException e) {
            logger.debug("The file source with hostname '{}' does not have the zip file '{}' with action '{}'",
                    new Object[]{fileSource.getHostname(),
                za.getZipId().getFileName(),
                za.getTodoId().getShortName()});
        } catch (NonUniqueResultException e) {
            logger.error(e.getMessage());
        } catch (IllegalStateException |
                QueryTimeoutException |
                TransactionRequiredException |
                PessimisticLockException |
                LockTimeoutException e) {
            logger.error(e.getMessage());
        } catch (PersistenceException e) {
            logger.error(e.getMessage());
        } finally {
            if (em.isOpen()) {
                em.close();
            }
        }
        return returnValue;
    }

    public synchronized FileSourceHasZip getFileSourceHasZip(ZipAction za) {
        FileSourceHasZip returnValue = null;
        EntityManager em = createEntityManager();
        try {
            TypedQuery<FileSourceHasZip> q = em.createNamedQuery("FileSourceHasZip.findByAction", FileSourceHasZip.class);
            q.setParameter(ZIP_ACTION_ID, za);
            returnValue = q.getSingleResult();
        } catch (NoResultException e) {
            logger.debug("No file source contains the zip file '{}' with action '{}'",
                    za.getZipId().getFileName(),
                    za.getTodoId().getShortName());
        } catch (NonUniqueResultException e) {
            logger.error(e.getMessage());
        } catch (IllegalStateException |
                QueryTimeoutException |
                TransactionRequiredException |
                PessimisticLockException |
                LockTimeoutException e) {
            logger.error(e.getMessage());
        } catch (PersistenceException e) {
            logger.error(e.getMessage());
        } finally {
            if (em.isOpen()) {
                em.close();
            }
        }
        return returnValue;
    }

    public synchronized Phase getPhase(String n) {
        Phase returnValue = null;
        EntityManager em = createEntityManager();
        try {
            TypedQuery<Phase> q = em.createNamedQuery("Phase.findByShortName", Phase.class);
            q.setParameter(SHORT_NAME, n);
            returnValue = q.getSingleResult();
        } catch (NoResultException e) {
            logger.error("There is no phase named '{}'!", n);
            exitCorruptDatabase();
        } catch (NonUniqueResultException e) {
            logger.error(e.getMessage());
        } catch (IllegalStateException |
                QueryTimeoutException |
                TransactionRequiredException |
                PessimisticLockException |
                LockTimeoutException e) {
            logger.error(e.getMessage());
        } catch (PersistenceException e) {
            logger.error(e.getMessage());
        } finally {
            if (em.isOpen()) {
                em.close();
            }
        }
        return returnValue;
    }

    public synchronized ResourceState getResourceState(String state) {
        ResourceState returnValue = null;
        EntityManager em = createEntityManager();
        try {
            TypedQuery<ResourceState> q = em.createNamedQuery("ResourceState.findByShortName", ResourceState.class);
            q.setParameter(SHORT_NAME, state);
            returnValue = q.getSingleResult();
        } catch (NoResultException e) {
            logger.debug("There is no resource state named '{}'", state);
        } catch (NonUniqueResultException e) {
            logger.error(e.getMessage());
        } catch (IllegalStateException |
                QueryTimeoutException |
                TransactionRequiredException |
                PessimisticLockException |
                LockTimeoutException e) {
            logger.error(e.getMessage());
        } catch (PersistenceException e) {
            logger.error(e.getMessage());
        } finally {
            if (em.isOpen()) {
                em.close();
            }
        }
        return returnValue;
    }

    public synchronized ProcessingType getProcessingType(String todo) {
        ProcessingType returnValue = null;
        EntityManager em = createEntityManager();
        try {
            TypedQuery<ProcessingType> q = em.createNamedQuery("ProcessingType.findByShortName", ProcessingType.class);
            q.setParameter(SHORT_NAME, todo);
            returnValue = q.getSingleResult();
        } catch (NoResultException e) {
            logger.debug("There is no processing type named '{}'", todo);
        } catch (NonUniqueResultException e) {
            logger.error(e.getMessage());
        } catch (IllegalStateException |
                QueryTimeoutException |
                TransactionRequiredException |
                PessimisticLockException |
                LockTimeoutException e) {
            logger.error(e.getMessage());
        } catch (PersistenceException e) {
            logger.error(e.getMessage());
        } finally {
            if (em.isOpen()) {
                em.close();
            }
        }
        return returnValue;
    }

    public synchronized SourceProtocol getSourceProtocol(String protocol) {
        SourceProtocol returnValue = null;
        EntityManager em = createEntityManager();
        try {
            TypedQuery<SourceProtocol> q = em.createNamedQuery("SourceProtocol.findByShortName", SourceProtocol.class);
            q.setParameter(SHORT_NAME, protocol);
            returnValue = q.getSingleResult();
        } catch (NoResultException e) {
            logger.debug("There is no source protocol named '{}'", protocol);
        } catch (NonUniqueResultException e) {
            logger.error(e.getMessage());
        } catch (IllegalStateException |
                QueryTimeoutException |
                TransactionRequiredException |
                PessimisticLockException |
                LockTimeoutException e) {
            logger.error(e.getMessage());
        } catch (PersistenceException e) {
            logger.error(e.getMessage());
        } finally {
            if (em.isOpen()) {
                em.close();
            }
        }
        return returnValue;
    }

    public synchronized AStatus getStatus(String n) {
        AStatus returnValue = null;
        EntityManager em = createEntityManager();
        try {
            TypedQuery<AStatus> q = em.createNamedQuery("AStatus.findByShortName", AStatus.class);
            q.setParameter(SHORT_NAME, n);
            returnValue = q.getSingleResult();
        } catch (NoResultException e) {
            logger.error("There is no phase status named '{}'!", n);
            exitCorruptDatabase();
        } catch (NonUniqueResultException e) {
            logger.error(e.getMessage());
        } catch (IllegalStateException |
                QueryTimeoutException |
                TransactionRequiredException |
                PessimisticLockException |
                LockTimeoutException e) {
            logger.error(e.getMessage());
        } catch (PersistenceException e) {
            logger.error(e.getMessage());
        } finally {
            if (em.isOpen()) {
                em.close();
            }
        }
        return returnValue;
    }

    public synchronized List<Centre> getCentres() {
        List<Centre> returnValue = null;
        EntityManager em = createEntityManager();
        try {
            TypedQuery<Centre> q = em.createNamedQuery("Centre.findAllActive", Centre.class);
            returnValue = q.getResultList();
        } catch (IllegalStateException |
                QueryTimeoutException |
                TransactionRequiredException |
                PessimisticLockException |
                LockTimeoutException e) {
            logger.error(e.getMessage());
        } catch (PersistenceException e) {
            logger.error(e.getMessage());
        } finally {
            if (em.isOpen()) {
                em.close();
            }
        }
        return returnValue;
    }

    public synchronized Centre getCentre(String shortName) {
        Centre returnValue = null;
        EntityManager em = createEntityManager();
        try {
            TypedQuery<Centre> q = em.createNamedQuery("Centre.findByShortName", Centre.class);
            q.setParameter("shortName", shortName);
            returnValue = q.getSingleResult();
        } catch (NoResultException e) {
            logger.error("There is no centre with short name '{}'!", shortName);
        } catch (IllegalStateException |
                QueryTimeoutException |
                TransactionRequiredException |
                PessimisticLockException |
                LockTimeoutException e) {
            logger.error(e.getMessage());
        } catch (PersistenceException e) {
            logger.error(e.getMessage());
        } finally {
            if (em.isOpen()) {
                em.close();
            }
        }
        return returnValue;
    }

    public synchronized List<FileSource> getFileSources(Centre c) {
        List<FileSource> returnValue = null;
        EntityManager em = createEntityManager();
        try {
            ResourceState rs = this.getResourceState(FileSource.AVAILABLE);
            if (rs != null) {
                TypedQuery<FileSource> q = em.createNamedQuery("FileSource.findByCentreState", FileSource.class);
                q.setParameter("centreId", c);
                q.setParameter("stateId", rs);
                returnValue = q.getResultList();
            }
        } catch (IllegalStateException |
                QueryTimeoutException |
                TransactionRequiredException |
                PessimisticLockException |
                LockTimeoutException e) {
            logger.error(e.getMessage());
        } catch (PersistenceException e) {
            logger.error(e.getMessage());
        } finally {
            if (em.isOpen()) {
                em.close();
            }
        }
        return returnValue;
    }

    public synchronized List<FileSourceHasZip> getFileSourceHasZip(EntityManager em, ZipAction za) {
        List<FileSourceHasZip> returnValue = null;
        try {
            TypedQuery<FileSourceHasZip> q = em.createNamedQuery("FileSourceHasZip.findByAction", FileSourceHasZip.class);
            q.setParameter(ZIP_ACTION_ID, za);
            returnValue = q.getResultList();
        } catch (IllegalStateException |
                QueryTimeoutException |
                TransactionRequiredException |
                PessimisticLockException |
                LockTimeoutException e) {
            logger.error(e.getMessage());
        } catch (PersistenceException e) {
            logger.error(e.getMessage());
        }
        return returnValue;
    }

    public synchronized List<FileSourceHasZip> getFileSourceHasZipCollection(ZipAction za) {
        List<FileSourceHasZip> returnValue = null;
        EntityManager em = createEntityManager();
        try {
            ZipAction zipAction = em.find(ZipAction.class, za.getId());
            if (zipAction == null) {
                logger.warn("Could not find zip action with id {}", za.getId());
            } else {
                returnValue = getFileSourceHasZip(em, za);
            }
        } finally {
            if (em.isOpen()) {
                em.close();
            }
        }
        return returnValue;
    }

    private synchronized Phase getPhase(EntityManager em, String n) {
        Phase returnValue = null;
        try {
            TypedQuery<Phase> q = em.createNamedQuery("Phase.findByShortName", Phase.class);
            q.setParameter(SHORT_NAME, n);
            returnValue = q.getSingleResult();
        } catch (NoResultException e) {
            logger.error("There is no phase named '{}'!", n);
            exitCorruptDatabase();
        } catch (NonUniqueResultException e) {
            logger.error(e.getMessage());
            exitCorruptDatabase();
        }
        return returnValue;
    }

    private synchronized AStatus getStatus(EntityManager em, String n) {
        AStatus returnValue = null;
        try {
            TypedQuery<AStatus> q = em.createNamedQuery("AStatus.findByShortName", AStatus.class);
            q.setParameter(SHORT_NAME, n);
            returnValue = q.getSingleResult();
        } catch (NoResultException e) {
            logger.error("There is no status named '{}'!", n);
            exitCorruptDatabase();
        } catch (NonUniqueResultException e) {
            logger.error(e.getMessage());
            exitCorruptDatabase();
        }
        return returnValue;
    }

    public synchronized boolean takeDownloadJob(ZipAction z) {
        boolean returnValue = false;
        EntityManager em = createEntityManager();
        try {
            ZipAction zipAction = em.find(ZipAction.class, z.getId());
            if (zipAction == null
                    || !(Phase.CHECK_ZIP_FILENAME.equals(zipAction.getPhaseId().getShortName())
                    && AStatus.DONE.equals(zipAction.getStatusId().getShortName()))) {
                returnValue = false;
            } else {
                Phase phase = getPhase(em, Phase.DOWNLOAD_FILE);
                AStatus status = getStatus(em, AStatus.RUNNING);
                em.getTransaction().begin();
                zipAction.setPhaseId(phase);
                zipAction.setStatusId(status);
                em.getTransaction().commit();
                returnValue = true;
            }
        } catch (EntityExistsException |
                IllegalArgumentException |
                TransactionRequiredException e) {
            logger.error(e.getMessage());
        } finally {
            if (em.isOpen()) {
                em.close();
            }
        }
        return returnValue;
    }

    public synchronized void persist(Object o) {
        EntityManager em = createEntityManager();
        try {
            em.getTransaction().begin();
            em.persist(o);
            em.getTransaction().commit();
        } catch (EntityExistsException |
                IllegalArgumentException |
                TransactionRequiredException e) {
            logger.error(e.getMessage());
        } finally {
            if (em.isOpen()) {
                em.close();
            }
        }
    }

    public synchronized void merge(Object o) {
        EntityManager em = createEntityManager();
        try {
            em.getTransaction().begin();
            em.merge(o);
            em.getTransaction().commit();
        } catch (IllegalArgumentException | TransactionRequiredException e) {
            logger.error(e.getMessage());
        } finally {
            if (em.isOpen()) {
                em.close();
            }
        }
    }

    public synchronized void refresh(Object o) {
        EntityManager em = createEntityManager();
        try {
            em.refresh(o);
        } catch (IllegalArgumentException |
                TransactionRequiredException |
                EntityNotFoundException e) {
            logger.error(e.getMessage());
        } finally {
            if (em.isOpen()) {
                em.close();
            }
        }
    }

    public synchronized void exitCorruptDatabase() {
        logger.error("Inconsistent database state.!");
        exit(1);
    }

    public synchronized CrawlingSession beginCrawling() {
        EntityManager em = createEntityManager();
        CrawlingSession session = new CrawlingSession();
        try {
            em.getTransaction().begin();
            em.persist(session);
            em.flush();
            em.refresh(session);
            em.getTransaction().commit();
        } catch (EntityExistsException |
                IllegalArgumentException |
                TransactionRequiredException e) {
            logger.error(e.getMessage());
        } finally {
            if (em.isOpen()) {
                em.close();
            }
        }
        return session;
    }

    public synchronized CrawlingSession finishCrawling(
            CrawlingSession session,
            short status) {
        EntityManager em = createEntityManager();
        try {
            if (session != null) {
                em.getTransaction().begin();
                session.setFinishTime(new Date());
                session.setStatus(status);
                em.merge(session);
                em.getTransaction().commit();
            }
        } catch (IllegalArgumentException | TransactionRequiredException e) {
            logger.error(e.getMessage());
        } finally {
            if (em.isOpen()) {
                em.close();
            }
        }
        return session;
    }

    public synchronized SessionTask beginSessionTask(
            CrawlingSession sessionId,
            Phase phaseId,
            String comment) {
        EntityManager em = createEntityManager();
        SessionTask task = null;
        try {
            if (phaseId != null && sessionId != null) {
                task = new SessionTask(phaseId, sessionId);
                if (comment != null) {
                    task.setComment(comment);
                }
                em.getTransaction().begin();
                em.persist(task);
                em.flush();
                em.refresh(task);
                em.getTransaction().commit();
            }
        } catch (EntityExistsException |
                IllegalArgumentException |
                TransactionRequiredException e) {
            logger.error(e.getMessage());
        } finally {
            if (em.isOpen()) {
                em.close();
            }
        }
        return task;
    }

    public synchronized SessionTask finishSessionTask(
            SessionTask task,
            short status) {
        EntityManager em = createEntityManager();
        try {
            if (task != null) {
                em.getTransaction().begin();
                task.setFinishTime(new Date());
                task.setStatus(status);
                em.merge(task);
                em.getTransaction().commit();
            }
        } catch (IllegalArgumentException | TransactionRequiredException e) {
            logger.error(e.getMessage());
        } finally {
            if (em.isOpen()) {
                em.close();
            }
        }
        return task;
    }
}
