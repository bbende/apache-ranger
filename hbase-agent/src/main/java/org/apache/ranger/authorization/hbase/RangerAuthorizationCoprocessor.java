/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.ranger.authorization.hbase;
import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TimeZone;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.CoprocessorException;
import org.apache.hadoop.hbase.coprocessor.CoprocessorService;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionServerCoprocessorEnvironment;
import org.apache.hadoop.hbase.filter.ByteArrayComparable;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.ipc.RequestContext;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.ResponseConverter;
import org.apache.hadoop.hbase.protobuf.generated.AccessControlProtos;
import org.apache.hadoop.hbase.protobuf.generated.AccessControlProtos.AccessControlService;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;
import org.apache.hadoop.hbase.protobuf.generated.SecureBulkLoadProtos.CleanupBulkLoadRequest;
import org.apache.hadoop.hbase.protobuf.generated.SecureBulkLoadProtos.PrepareBulkLoadRequest;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.regionserver.ScanType;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.security.AccessDeniedException;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.access.Permission;
import org.apache.hadoop.hbase.security.access.Permission.Action;
import org.apache.hadoop.hbase.security.access.RangerAccessControlLists;
import org.apache.hadoop.hbase.security.access.TablePermission;
import org.apache.hadoop.hbase.security.access.UserPermission;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.ranger.admin.client.RangerAdminRESTClient;
import org.apache.ranger.admin.client.datatype.GrantRevokeData;
import org.apache.ranger.admin.client.datatype.GrantRevokeData.PermMap;
import org.apache.ranger.audit.model.AuthzAuditEvent;
import org.apache.ranger.authorization.hadoop.config.RangerConfiguration;
import org.apache.ranger.authorization.hadoop.constants.RangerHadoopConstants;
import org.apache.ranger.authorization.utils.StringUtil;
import org.apache.ranger.plugin.audit.RangerDefaultAuditHandler;
import org.apache.ranger.plugin.policyengine.RangerPolicyEngine;
import org.apache.ranger.plugin.service.RangerBasePlugin;

import com.google.common.base.Objects;
import com.google.common.collect.Lists;
import com.google.common.collect.MapMaker;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;

public class RangerAuthorizationCoprocessor extends RangerAuthorizationCoprocessorBase implements AccessControlService.Interface, CoprocessorService {
	private static final Log LOG = LogFactory.getLog(RangerAuthorizationCoprocessor.class.getName());
	private static final String repositoryName          = RangerConfiguration.getInstance().get(RangerHadoopConstants.AUDITLOG_REPOSITORY_NAME_PROP);
	private static final boolean UpdateRangerPoliciesOnGrantRevoke = RangerConfiguration.getInstance().getBoolean(RangerHadoopConstants.HBASE_UPDATE_RANGER_POLICIES_ON_GRANT_REVOKE_PROP, RangerHadoopConstants.HBASE_UPDATE_RANGER_POLICIES_ON_GRANT_REVOKE_DEFAULT_VALUE);
	private static final String GROUP_PREFIX = "@";
		
	private static final String SUPERUSER_CONFIG_PROP = "hbase.superuser";
	private static final String WILDCARD = "*";
	
    private static final TimeZone gmtTimeZone = TimeZone.getTimeZone("GMT+0");

    private RegionCoprocessorEnvironment regionEnv;
	private Map<InternalScanner, String> scannerOwners = new MapMaker().weakKeys().makeMap();
	
	private List<String> superUserList = null;

	/*
	 * These are package level only for testability and aren't meant to be exposed outside via getters/setters or made available to derived classes.
	 */
	final HbaseFactory _factory = HbaseFactory.getInstance();
	final RangerPolicyEngine _authorizer = _factory.getPolicyEngine();
	final HbaseUserUtils _userUtils = _factory.getUserUtils();
	final HbaseAuthUtils _authUtils = _factory.getAuthUtils();
	
	// Utilities Methods 
	protected byte[] getTableName(RegionCoprocessorEnvironment e) {
		HRegion region = e.getRegion();
		byte[] tableName = null;
		if (region != null) {
			HRegionInfo regionInfo = region.getRegionInfo();
			if (regionInfo != null) {
				tableName = regionInfo.getTable().getName() ;
			}
		}
		return tableName;
	}
	protected void requireSystemOrSuperUser(Configuration conf) throws IOException {
		User user = User.getCurrent();
		if (user == null) {
			throw new IOException("Unable to obtain the current user, authorization checks for internal operations will not work correctly!");
		}
		String currentUser = user.getShortName();
		List<String> superusers = Lists.asList(currentUser, conf.getStrings(SUPERUSER_CONFIG_PROP, new String[0]));
		User activeUser = getActiveUser();
		if (!(superusers.contains(activeUser.getShortName()))) {
			throw new AccessDeniedException("User '" + (user != null ? user.getShortName() : "null") + "is not system or super user.");
		}
	}
	private boolean isSuperUser(User user) {
		boolean isSuper = false;
		isSuper = (superUserList != null && superUserList.contains(user.getShortName()));
		if (LOG.isDebugEnabled()) {
			LOG.debug("IsSuperCheck on [" + user.getShortName() + "] returns [" + isSuper + "]");
		}
		return isSuper;
	}
	protected boolean isSpecialTable(HRegionInfo regionInfo) {
		return isSpecialTable(regionInfo.getTable().getName());
	}
	protected boolean isSpecialTable(byte[] tableName) {
		return isSpecialTable(Bytes.toString(tableName));
	}
	protected boolean isSpecialTable(String tableNameStr) {
		return tableNameStr.equals("hbase:meta") ||  tableNameStr.equals("-ROOT-") || tableNameStr.equals(".META.");
	}
	protected boolean isAccessForMetaTables(RegionCoprocessorEnvironment env) {
		HRegionInfo hri = env.getRegion().getRegionInfo();
		
		if (hri.isMetaTable() || hri.isMetaRegion()) {
			return true;
		} else {
			return false;
		}
	}

	private User getActiveUser() {
		User user = RequestContext.getRequestUser();
		if (!RequestContext.isInRequestContext()) {
			// for non-rpc handling, fallback to system user
			try {
				user = User.getCurrent();
			} catch (IOException e) {
				LOG.error("Unable to find the current user");
				user = null;
			}
		}
		return user;
	}
	
	private String getRemoteAddress() {
		RequestContext reqContext = RequestContext.get();
		InetAddress    remoteAddr = reqContext != null ? reqContext.getRemoteAddress() : null;
		String         strAddr    = remoteAddr != null ? remoteAddr.getHostAddress() : null;

		return strAddr;
	}

	// Methods that are used within the CoProcessor 
	private void requireScannerOwner(InternalScanner s) throws AccessDeniedException {
		if (RequestContext.isInRequestContext()) {
			String requestUserName = RequestContext.getRequestUserName();
			String owner = scannerOwners.get(s);
			if (owner != null && !owner.equals(requestUserName)) {
				throw new AccessDeniedException("User '" + requestUserName + "' is not the scanner owner!");
			}
		}
	}

	/**
	 * @param families
	 * @return empty map if families is null, would never have empty or null keys, would never have null values, values could be empty (non-null) set
	 */
	Map<String, Set<String>> getColumnFamilies(Map<byte[], ? extends Collection<?>> families) {
		if (families == null) {
			// null families map passed.  Ok, returning empty map.
			return Collections.<String, Set<String>>emptyMap();
		}
		Map<String, Set<String>> result = new HashMap<String, Set<String>>();
		for (Map.Entry<byte[], ? extends Collection<?>> anEntry : families.entrySet()) {
			byte[] familyBytes = anEntry.getKey();
			String family = Bytes.toString(familyBytes);
			if (family == null || family.isEmpty()) {
				LOG.error("Unexpected Input: got null or empty column family (key) in families map! Ignoring...");
			} else {
				Collection<?> columnCollection = anEntry.getValue();
				if (CollectionUtils.isEmpty(columnCollection)) {
					// family points to null map, OK.
					result.put(family, Collections.<String> emptySet());
				} else {
					Iterator<String> columnIterator = new ColumnIterator(columnCollection);
					Set<String> columns = new HashSet<String>();
					while (columnIterator.hasNext()) {
						String column = columnIterator.next();
						columns.add(column);
					}
					result.put(family, columns);
				}
			}
		}
		return result;
	}
	
	static class ColumnFamilyAccessResult {
		final boolean _everythingIsAccessible;
		final boolean _somethingIsAccessible;
		final List<AuthzAuditEvent> _accessAllowedEvents;
		final AuthzAuditEvent _accessDeniedEvent;
		final Map<String, Set<String>> _allowedColumns;
		final String _denialReason;
		
		ColumnFamilyAccessResult(
				boolean everythingIsAccessible, boolean somethingIsAccessible, 
				List<AuthzAuditEvent> accessAllowedEvents, AuthzAuditEvent accessDeniedEvent,
				Map<String, Set<String>> allowedColumns, String denialReason) {
			_everythingIsAccessible = everythingIsAccessible;
			_somethingIsAccessible = somethingIsAccessible;
			// WARNING: we are just holding on to reference of the collection.  Potentially risky optimization
			_accessAllowedEvents = accessAllowedEvents;
			_accessDeniedEvent = accessDeniedEvent;
			_allowedColumns = allowedColumns;
			_denialReason = denialReason;
		}
		
		@Override
		public String toString() {
			return Objects.toStringHelper(getClass())
					.add("everythingIsAccessible", _everythingIsAccessible)
					.add("somethingIsAccessible", _somethingIsAccessible)
					.add("accessAllowedEvents", _accessAllowedEvents)
					.add("accessDeniedEvent", _accessDeniedEvent)
					.add("allowedColumns", _allowedColumns)
					.add("denialReason", _denialReason)
					.toString();
			
		}
	}
	
	ColumnFamilyAccessResult evaluateAccess(String operation, Action action, final RegionCoprocessorEnvironment env, 
			final Map<byte[], ? extends Collection<?>> familyMap) throws AccessDeniedException {
		
		String access = _authUtils.getAccess(action);
		
		if (LOG.isDebugEnabled()) {
			final String format = "evaluateAccess: entered: Operation[%s], access[%s], families[%s]";
			Map<String, Set<String>> families = getColumnFamilies(familyMap);
			String message = String.format(format, operation, access, families.toString());
			LOG.debug(message);
		}

		byte[] tableBytes = getTableName(env);
		if (tableBytes == null || tableBytes.length == 0) {
			String message = "evaluateAccess: Unexpected: Couldn't get table from RegionCoprocessorEnvironment. Access denied, not audited";
			LOG.debug(message);
			throw new AccessDeniedException("Insufficient permissions for operation '" + operation + "',action: " + action);
		}
		String table = Bytes.toString(tableBytes);

		final String messageTemplate = "evaluateAccess: exiting: Operation[%s], access[%s], families[%s], verdict[%s]";
		ColumnFamilyAccessResult result;
		if (canSkipAccessCheck(operation, access, table) || canSkipAccessCheck(operation, access, env)) {
			LOG.debug("evaluateAccess: exiting: isKnownAccessPattern returned true: access allowed, not audited");
			result = new ColumnFamilyAccessResult(true, true, null, null, null, null);
			if (LOG.isDebugEnabled()) {
				Map<String, Set<String>> families = getColumnFamilies(familyMap);
				String message = String.format(messageTemplate, operation, access, families.toString(), result.toString());
				LOG.debug(message);
			}
			return result;
		}
		
		User user = getActiveUser();
		// let's create a session that would be reused.  Set things on it that won't change.
		HbaseAuditHandler auditHandler = _factory.getAuditHandler(); 
		AuthorizationSession session = new AuthorizationSession(_authorizer)
				.operation(operation)
				.remoteAddress(getRemoteAddress())
				.auditHandler(auditHandler)
				.user(user)
				.access(access)
				.table(table);
		Map<String, Set<String>> families = getColumnFamilies(familyMap);
		if (LOG.isDebugEnabled()) {
			LOG.debug("evaluateAccess: families to process: " + families.toString());
		}
		if (families == null || families.isEmpty()) {
			LOG.debug("evaluateAccess: Null or empty families collection, ok.  Table level access is desired");
			session.buildRequest()
				.authorize();
			boolean authorized = session.isAuthorized();
			String reason = "";
			if (!authorized) {
				reason = String.format("Insufficient permissions for user ‘%s',action: %s, tableName:%s, no column families found.", user.getName(), operation, table);
			}
			AuthzAuditEvent event = auditHandler.discardMostRecentEvent(); // this could be null, of course, depending on audit settings of table.

			// if authorized then pass captured events as access allowed set else as access denied set.
			result = new ColumnFamilyAccessResult(authorized, authorized, 
						authorized ? Collections.singletonList(event) : null,
						authorized ? null : event, null, reason); 
			if (LOG.isDebugEnabled()) {
				String message = String.format(messageTemplate, operation, access, families.toString(), result.toString());
				LOG.debug(message);
			}
			return result;
		} else {
			LOG.debug("evaluateAccess: Families collection not null.  Skipping table-level check, will do finer level check");
		}
		
		boolean everythingIsAccessible = true;
		boolean somethingIsAccessible = false;
		// we would have to accumulate audits of all successful accesses and any one denial (which in our case ends up being the last denial)
		List<AuthzAuditEvent> authorizedEvents = new ArrayList<AuthzAuditEvent>(); 
		AuthzAuditEvent deniedEvent = null;
		String denialReason = null;
		// we need to cache the auths results so that we can create a filter, if needed
		Map<String, Set<String>> accessResultsCache = new HashMap<String, Set<String>>();
		
		for (Map.Entry<String, Set<String>> anEntry : families.entrySet()) {
			String family = anEntry.getKey();
			session.columnFamily(family);
			if (LOG.isDebugEnabled()) {
				LOG.debug("evaluateAccess: Processing family: " + family);
			}
			Set<String> columns = anEntry.getValue();
			if (columns == null || columns.isEmpty()) {
				LOG.debug("evaluateAccess: columns collection null or empty, ok.  Family level access is desired.");
				session.column(null) // zap stale column from prior iteration of this loop, if any
					.buildRequest()
					.authorize();
				if (session.isAuthorized()) {
					// we need to do 3 things: housekeeping, capturing audit events, building the results cache for filter
					somethingIsAccessible = true;
					AuthzAuditEvent event = auditHandler.discardMostRecentEvent();
					if (event != null) {
						authorizedEvents.add(event);
					}
					// presence of key with null value would imply access to all columns in a family.
					accessResultsCache.put(family, null);
				} else {
					everythingIsAccessible = false;
					deniedEvent = auditHandler.discardMostRecentEvent();
					denialReason = String.format("Insufficient permissions for user ‘%s',action: %s, tableName:%s, family:%s, no columns found.", user.getName(), operation, table, family);
				}
			} else {
				LOG.debug("evaluateAccess: columns collection not empty.  Skipping Family level check, will do finer level access check.");
				Set<String> accessibleColumns = new HashSet<String>(); // will be used in to populate our results cache for the filter
 				for (String column : columns) {
 					if (LOG.isDebugEnabled()) {
 						LOG.debug("evaluateAccess: Processing column: " + column);
 					}
 					session.column(column)
 						.buildRequest()
 						.authorize();
 					if (session.isAuthorized()) {
 						// we need to do 3 things: housekeeping, capturing audit events, building the results cache for filter
 						somethingIsAccessible = true;
 						AuthzAuditEvent event = auditHandler.discardMostRecentEvent();
 						if (event != null) {
 							authorizedEvents.add(event);
 						}
 						accessibleColumns.add(column);
 					} else {
 						everythingIsAccessible = false;
 						deniedEvent = auditHandler.discardMostRecentEvent();
 						denialReason = String.format("Insufficient permissions for user ‘%s',action: %s, tableName:%s, family:%s, column: %s", user.getName(), operation, table, family, column);  
 					}
				}
 				if (!accessibleColumns.isEmpty()) {
 					accessResultsCache.put(family, accessibleColumns);
 				}
			}
		}
		
		result = new ColumnFamilyAccessResult(everythingIsAccessible, somethingIsAccessible, authorizedEvents, deniedEvent, accessResultsCache, denialReason);
		if (LOG.isDebugEnabled()) {
			String message = String.format(messageTemplate, operation, access, families.toString(), result.toString());
			LOG.debug(message);
		}
		return result;
	}
	
	Filter authorizeAccess(String operation, Action action, final RegionCoprocessorEnvironment env, final Map<byte[], NavigableSet<byte[]>> familyMap) throws AccessDeniedException {

		ColumnFamilyAccessResult accessResult = evaluateAccess(operation, action, env, familyMap);
		RangerDefaultAuditHandler auditHandler = new RangerDefaultAuditHandler();
		if (accessResult._everythingIsAccessible) {
			auditHandler.logAuthzAudits(accessResult._accessAllowedEvents);
			LOG.debug("authorizeAccess: exiting: No filter returned since all access was allowed");
			return null; // no filter needed since we are good to go.
		} else if (accessResult._somethingIsAccessible) {
			auditHandler.logAuthzAudits(accessResult._accessAllowedEvents); // we still need to log those to which we got access.
			LOG.debug("authorizeAccess: exiting: Filter returned since some access was allowed");
			return new RangerAuthorizationFilter(accessResult._allowedColumns);
		} else {
			// If we are here then it means nothing was accessible!  So let's log one denial (in our case, the last denial) and throw an exception
			auditHandler.logAuthzAudit(accessResult._accessDeniedEvent);
			LOG.debug("authorizeAccess: exiting: Throwing exception since nothing was accessible");
			throw new AccessDeniedException(accessResult._denialReason);
		}
	}
	
	Filter combineFilters(Filter filter, Filter existingFilter) {
		Filter combinedFilter = filter;
		if (existingFilter != null) {
			combinedFilter = new FilterList(FilterList.Operator.MUST_PASS_ALL, Lists.newArrayList(filter, existingFilter));
		}
		return combinedFilter;
	}

	void requirePermission(final String operation, final Action action, final RegionCoprocessorEnvironment regionServerEnv, final Map<byte[], ? extends Collection<?>> familyMap) 
			throws AccessDeniedException {

		ColumnFamilyAccessResult accessResult = evaluateAccess(operation, action, regionServerEnv, familyMap);
		RangerDefaultAuditHandler auditHandler = new RangerDefaultAuditHandler();
		if (accessResult._everythingIsAccessible) {
			auditHandler.logAuthzAudits(accessResult._accessAllowedEvents);
			LOG.debug("requirePermission: exiting: all access was allowed");
			return;
		} else {
			auditHandler.logAuthzAudit(accessResult._accessDeniedEvent);
			LOG.debug("requirePermission: exiting: throwing exception as everything wasn't accessible");
			throw new AccessDeniedException(accessResult._denialReason);
		}
	}
	
	/**
	 * This could run s
	 * @param operation
	 * @param otherInformation
	 * @param access
	 * @param table
	 * @param columnFamily
	 * @param column
	 * @return
	 * @throws AccessDeniedException 
	 */
	void authorizeAccess(String operation, String otherInformation, Action action, String table, String columnFamily, String column) throws AccessDeniedException {
		
		String access = _authUtils.getAccess(action);
		if (LOG.isDebugEnabled()) {
			final String format = "authorizeAccess: %s: Operation[%s], Info[%s], access[%s], table[%s], columnFamily[%s], column[%s]";
			String message = String.format(format, "Entering", operation, otherInformation, access, table, columnFamily, column);
			LOG.debug(message);
		}
		
		final String format =  "authorizeAccess: %s: Operation[%s], Info[%s], access[%s], table[%s], columnFamily[%s], column[%s], allowed[%s], reason[%s]";
		if (canSkipAccessCheck(operation, access, table)) {
			if (LOG.isDebugEnabled()) {
				String message = String.format(format, "Exiting", operation, otherInformation, access, table, columnFamily, column, true, "can skip auth check");
				LOG.debug(message);
			}
			return;
		}
		User user = getActiveUser();
		
		HbaseAuditHandler auditHandler = _factory.getAuditHandler(); 
		AuthorizationSession session = new AuthorizationSession(_authorizer)
			.operation(operation)
			.otherInformation(otherInformation)
			.remoteAddress(getRemoteAddress())
			.auditHandler(auditHandler)
			.user(user)
			.access(access)
			.table(table)
			.columnFamily(columnFamily)
			.column(column)
			.buildRequest()
			.authorize();
		
		if (LOG.isDebugEnabled()) {
			boolean allowed = session.isAuthorized();
			String reason = session.getDenialReason();
			String message = String.format(format, "Exiting", operation, otherInformation, access, table, columnFamily, column, allowed, reason);
			LOG.debug(message);
		}
		
		session.publishResults();
	}
	
	boolean canSkipAccessCheck(final String operation, String access, final String table) 
			throws AccessDeniedException {
		
		User user = getActiveUser();
		boolean result = false;
		if (user == null) {
			String message = "Unexpeceted: User is null: access denied, not audited!"; 
			LOG.warn("canSkipAccessCheck: exiting" + message);
			throw new AccessDeniedException("No user associated with request (" + operation + ") for action: " + access + "on table:" + table);
		} else if (isSuperUser(user)) {
			LOG.debug("canSkipAccessCheck: true: superuser access allowed, not audited");
			result = true;
		} else if (isAccessForMetadataRead(access, table)) {
			LOG.debug("canSkipAccessCheck: true: metadata read access always allowed, not audited");
			result = true;
		} else {
			LOG.debug("Can't skip access checks");
		}
		
		return result;
	}
	
	boolean canSkipAccessCheck(final String operation, String access, final RegionCoprocessorEnvironment regionServerEnv) throws AccessDeniedException {

		User user = getActiveUser();
		// read access to metadata tables is always allowed and isn't audited.
		if (isAccessForMetaTables(regionServerEnv) && _authUtils.isReadAccess(access)) {
			LOG.debug("isKnownAccessPattern: exiting: Read access for metadata tables allowed, not audited!");
			return true;
		}
		// if write access is desired to metatables then global create access is sufficient
		if (_authUtils.isWriteAccess(access) && isAccessForMetaTables(regionServerEnv)) {
			String createAccess = _authUtils.getAccess(Action.CREATE);
			AuthorizationSession session = new AuthorizationSession(_authorizer)
				.operation(operation)
				.remoteAddress(getRemoteAddress())
				.user(user)
				.access(createAccess)
				.buildRequest()
				.authorize();
			if (session.isAuthorized()) {
				// NOTE: this access isn't logged
				LOG.debug("isKnownAccessPattern: exiting: User has global create access, allowed!");
				return true;
			}
		}
		return false;
	}
	
	boolean isAccessForMetadataRead(String access, String table) {
		if (_authUtils.isReadAccess(access) && isSpecialTable(table)) {
			LOG.debug("isAccessForMetadataRead: Metadata tables read: access allowed!");
			return true;
		}
		return false;
	}

	// Check if the user has global permission ...
	protected void requireGlobalPermission(String request, String objName, Permission.Action action) throws AccessDeniedException {
		authorizeAccess(request, objName, action, null, null, null);
	}

	protected void requirePermission(String request, byte[] tableName, Permission.Action action) throws AccessDeniedException {
		String table = Bytes.toString(tableName);

		authorizeAccess(request, null, action, table, null, null);
	}
	
	protected void requirePermission(String request, byte[] aTableName, byte[] aColumnFamily, byte[] aQualifier, Permission.Action action) throws AccessDeniedException {

		String table = Bytes.toString(aTableName);
		String columnFamily = Bytes.toString(aColumnFamily);
		String column = Bytes.toString(aQualifier);

		authorizeAccess(request, null, action, table, columnFamily, column);
	}
	
	protected void requirePermission(String request, Permission.Action perm, RegionCoprocessorEnvironment env, Collection<byte[]> families) throws IOException {
		HashMap<byte[], Set<byte[]>> familyMap = new HashMap<byte[], Set<byte[]>>();

		if(families != null) {
			for (byte[] family : families) {
				familyMap.put(family, null);
			}
		}
		requirePermission(request, perm, env, familyMap);
	}
	
	public void checkPermissions(Permission[] permissions) throws IOException {
		String tableName = regionEnv.getRegion().getTableDesc().getTableName().getNameAsString() ;
		for (Permission permission : permissions) {
			if (permission instanceof TablePermission) {
				TablePermission tperm = (TablePermission) permission;
				for (Permission.Action action : permission.getActions()) {
					if (! tperm.getTableName().getNameAsString().equals(tableName)) {
						throw new AccessDeniedException(String.format("This method can only execute at the table specified in TablePermission. " + "Table of the region:%s , requested table:%s", tableName, 
																	  tperm.getTableName().getNameAsString()));
					}
					HashMap<byte[], Set<byte[]>> familyMap = Maps.newHashMapWithExpectedSize(1);
					if (tperm.getFamily() != null) {
						if (tperm.getQualifier() != null) {
							familyMap.put(tperm.getFamily(), Sets.newHashSet(tperm.getQualifier()));
						} else {
							familyMap.put(tperm.getFamily(), null);
						}
					}
					requirePermission("checkPermissions", action, regionEnv, familyMap);
				}
			} else {
				for (Permission.Action action : permission.getActions()) {
					byte[] tname = regionEnv.getRegion().getTableDesc().getTableName().getName() ;
					requirePermission("checkPermissions", tname, action);
				}
			}
		}
	}
	
	@Override
	public void postScannerClose(ObserverContext<RegionCoprocessorEnvironment> c, InternalScanner s) throws IOException {
		scannerOwners.remove(s);
	}
	@Override
	public RegionScanner postScannerOpen(ObserverContext<RegionCoprocessorEnvironment> c, Scan scan, RegionScanner s) throws IOException {
		User user = getActiveUser();
		if (user != null && user.getShortName() != null) {
			scannerOwners.put(s, user.getShortName());
		}
		return s;
	}
	@Override
	public void postStartMaster(ObserverContext<MasterCoprocessorEnvironment> ctx) throws IOException {
		if(UpdateRangerPoliciesOnGrantRevoke) {
			RangerAccessControlLists.init(ctx.getEnvironment().getMasterServices());
		}
	}
	@Override
	public void preAddColumn(ObserverContext<MasterCoprocessorEnvironment> c, TableName tableName, HColumnDescriptor column) throws IOException {
		requirePermission("addColumn", tableName.getName(), null, null, Action.CREATE);
	}
	@Override
	public Result preAppend(ObserverContext<RegionCoprocessorEnvironment> c, Append append) throws IOException {
		requirePermission("append", TablePermission.Action.WRITE, c.getEnvironment(), append.getFamilyCellMap());
		return null;
	}
	@Override
	public void preAssign(ObserverContext<MasterCoprocessorEnvironment> c, HRegionInfo regionInfo) throws IOException {
		requirePermission("assign", regionInfo.getTable().getName(), null, null, Action.ADMIN);
	}
	@Override
	public void preBalance(ObserverContext<MasterCoprocessorEnvironment> c) throws IOException {
		requirePermission("balance", null, Permission.Action.ADMIN);
	}
	@Override
	public boolean preBalanceSwitch(ObserverContext<MasterCoprocessorEnvironment> c, boolean newValue) throws IOException {
		requirePermission("balanceSwitch", null, Permission.Action.ADMIN);
		return newValue;
	}
	@Override
	public void preBulkLoadHFile(ObserverContext<RegionCoprocessorEnvironment> ctx, List<Pair<byte[], String>> familyPaths) throws IOException {
		List<byte[]> cfs = new LinkedList<byte[]>();
		for (Pair<byte[], String> el : familyPaths) {
			cfs.add(el.getFirst());
		}
		requirePermission("bulkLoadHFile", Permission.Action.WRITE, ctx.getEnvironment(), cfs);
	}
	@Override
	public boolean preCheckAndDelete(ObserverContext<RegionCoprocessorEnvironment> c, byte[] row, byte[] family, byte[] qualifier, CompareOp compareOp, ByteArrayComparable comparator, Delete delete, boolean result) throws IOException {
		Collection<byte[]> familyMap = Arrays.asList(new byte[][] { family });
		requirePermission("checkAndDelete", TablePermission.Action.READ, c.getEnvironment(), familyMap);
		requirePermission("checkAndDelete", TablePermission.Action.WRITE, c.getEnvironment(), familyMap);
		return result;
	}
	@Override
	public boolean preCheckAndPut(ObserverContext<RegionCoprocessorEnvironment> c, byte[] row, byte[] family, byte[] qualifier, CompareOp compareOp, ByteArrayComparable comparator, Put put, boolean result) throws IOException {
		Collection<byte[]> familyMap = Arrays.asList(new byte[][] { family });
		requirePermission("checkAndPut", TablePermission.Action.READ, c.getEnvironment(), familyMap);
		requirePermission("checkAndPut", TablePermission.Action.WRITE, c.getEnvironment(), familyMap);
		return result;
	}
	@Override
	public void preCloneSnapshot(ObserverContext<MasterCoprocessorEnvironment> ctx, SnapshotDescription snapshot, HTableDescriptor hTableDescriptor) throws IOException {
		requirePermission("cloneSnapshot", null, Permission.Action.ADMIN);
	}
	@Override
	public void preClose(ObserverContext<RegionCoprocessorEnvironment> e, boolean abortRequested) throws IOException {
		requirePermission("close", getTableName(e.getEnvironment()), Permission.Action.ADMIN);
	}
	@Override
	public InternalScanner preCompact(ObserverContext<RegionCoprocessorEnvironment> e, Store store, InternalScanner scanner,ScanType scanType) throws IOException {
		requirePermission("compact", getTableName(e.getEnvironment()), null, null, Action.CREATE);
		return scanner;
	}
	@Override
	public void preCompactSelection(ObserverContext<RegionCoprocessorEnvironment> e, Store store, List<StoreFile> candidates) throws IOException {
		requirePermission("compactSelection", getTableName(e.getEnvironment()), null, null, Action.CREATE);
	}

	@Override
	public void preCreateTable(ObserverContext<MasterCoprocessorEnvironment> c, HTableDescriptor desc, HRegionInfo[] regions) throws IOException {
		requirePermission("createTable", desc.getName(), Permission.Action.CREATE);
	}
	@Override
	public void preDelete(ObserverContext<RegionCoprocessorEnvironment> c, Delete delete, WALEdit edit, Durability durability) throws IOException {
		requirePermission("delete", TablePermission.Action.WRITE, c.getEnvironment(), delete.getFamilyCellMap());
	}
	@Override
	public void preDeleteColumn(ObserverContext<MasterCoprocessorEnvironment> c, TableName tableName, byte[] col) throws IOException {
		requirePermission("deleteColumn", tableName.getName(), null, null, Action.CREATE);
	}
	@Override
	public void preDeleteSnapshot(ObserverContext<MasterCoprocessorEnvironment> ctx, SnapshotDescription snapshot) throws IOException {
		requirePermission("deleteSnapshot", null, Permission.Action.ADMIN);
	}
	@Override
	public void preDeleteTable(ObserverContext<MasterCoprocessorEnvironment> c, TableName tableName) throws IOException {
		requirePermission("deleteTable", tableName.getName(), null, null, Action.CREATE);
	}
	@Override
	public void preDisableTable(ObserverContext<MasterCoprocessorEnvironment> c, TableName tableName) throws IOException {
		requirePermission("disableTable", tableName.getName(), null, null, Action.CREATE);
	}
	@Override
	public void preEnableTable(ObserverContext<MasterCoprocessorEnvironment> c, TableName tableName) throws IOException {
		requirePermission("enableTable", tableName.getName(), null, null, Action.CREATE);
	}
	@Override
	public boolean preExists(ObserverContext<RegionCoprocessorEnvironment> c, Get get, boolean exists) throws IOException {
		requirePermission("exists", TablePermission.Action.READ, c.getEnvironment(), get.familySet());
		return exists;
	}
	@Override
	public void preFlush(ObserverContext<RegionCoprocessorEnvironment> e) throws IOException {
		requirePermission("flush", getTableName(e.getEnvironment()), null, null, Action.CREATE);
	}
	@Override
	public void preGetClosestRowBefore(ObserverContext<RegionCoprocessorEnvironment> c, byte[] row, byte[] family, Result result) throws IOException {
		requirePermission("getClosestRowBefore", TablePermission.Action.READ, c.getEnvironment(), (family != null ? Lists.newArrayList(family) : null));
	}
	@Override
	public Result preIncrement(ObserverContext<RegionCoprocessorEnvironment> c, Increment increment) throws IOException {
		requirePermission("increment", TablePermission.Action.WRITE, c.getEnvironment(), increment.getFamilyCellMap().keySet());
		
		return null;
	}
	@Override
	public long preIncrementColumnValue(ObserverContext<RegionCoprocessorEnvironment> c, byte[] row, byte[] family, byte[] qualifier, long amount, boolean writeToWAL) throws IOException {
		requirePermission("incrementColumnValue", TablePermission.Action.READ, c.getEnvironment(), Arrays.asList(new byte[][] { family }));
		requirePermission("incrementColumnValue", TablePermission.Action.WRITE, c.getEnvironment(), Arrays.asList(new byte[][] { family }));
		return -1;
	}
	@Override
	public void preModifyColumn(ObserverContext<MasterCoprocessorEnvironment> c, TableName tableName, HColumnDescriptor descriptor) throws IOException {
		requirePermission("modifyColumn", tableName.getName(), null, null, Action.CREATE);
	}
	@Override
	public void preModifyTable(ObserverContext<MasterCoprocessorEnvironment> c, TableName tableName, HTableDescriptor htd) throws IOException {
		requirePermission("modifyTable", tableName.getName(), null, null, Action.CREATE);
	}
	@Override
	public void preMove(ObserverContext<MasterCoprocessorEnvironment> c, HRegionInfo region, ServerName srcServer, ServerName destServer) throws IOException {
		requirePermission("move", region.getTable().getName() , null, null, Action.ADMIN);
	}
	@Override
	public void preOpen(ObserverContext<RegionCoprocessorEnvironment> e) throws IOException {
		RegionCoprocessorEnvironment env = e.getEnvironment();
		final HRegion region = env.getRegion();
		if (region == null) {
			LOG.error("NULL region from RegionCoprocessorEnvironment in preOpen()");
			return;
		} else {
			HRegionInfo regionInfo = region.getRegionInfo();
			if (isSpecialTable(regionInfo)) {
				requireSystemOrSuperUser(regionEnv.getConfiguration());
			} else {
				requirePermission("open", getTableName(e.getEnvironment()), Action.ADMIN);
			}
		}
	}
	@Override
	public void preRestoreSnapshot(ObserverContext<MasterCoprocessorEnvironment> ctx, SnapshotDescription snapshot, HTableDescriptor hTableDescriptor) throws IOException {
		requirePermission("restoreSnapshot", hTableDescriptor.getName(), Permission.Action.ADMIN);
	}

	@Override
	public void preScannerClose(ObserverContext<RegionCoprocessorEnvironment> c, InternalScanner s) throws IOException {
		requireScannerOwner(s);
	}
	@Override
	public boolean preScannerNext(ObserverContext<RegionCoprocessorEnvironment> c, InternalScanner s, List<Result> result, int limit, boolean hasNext) throws IOException {
		requireScannerOwner(s);
		return hasNext;
	}
	@Override
	public RegionScanner preScannerOpen(ObserverContext<RegionCoprocessorEnvironment> c, Scan scan, RegionScanner s) throws IOException {
		RegionCoprocessorEnvironment e = c.getEnvironment();
		
		Map<byte[], NavigableSet<byte[]>> familyMap = scan.getFamilyMap();
		String operation = "scannerOpen";
		Filter filter = authorizeAccess(operation, Action.READ, e, familyMap);
		if (filter == null) {
			if (LOG.isDebugEnabled()) {
				LOG.debug("preGetOp: Access allowed for all families/column.");
			}
		} else {
			if (LOG.isDebugEnabled()) {
				LOG.debug("preGetOp: Access allowed for some of the families/column.");
			}
			Filter existingFilter = scan.getFilter();
			Filter combinedFilter = combineFilters(filter, existingFilter);
			scan.setFilter(combinedFilter);
		}
		return s;
	}
	@Override
	public void preShutdown(ObserverContext<MasterCoprocessorEnvironment> c) throws IOException {
		requirePermission("shutdown", null, Permission.Action.ADMIN);
	}
	@Override
	public void preSnapshot(ObserverContext<MasterCoprocessorEnvironment> ctx, SnapshotDescription snapshot, HTableDescriptor hTableDescriptor) throws IOException {
		requirePermission("snapshot", hTableDescriptor.getName(), Permission.Action.ADMIN);
	}
	@Override
	public void preSplit(ObserverContext<RegionCoprocessorEnvironment> e) throws IOException {
		requirePermission("split", getTableName(e.getEnvironment()), null, null, Action.ADMIN);
	}
	@Override
	public void preStopMaster(ObserverContext<MasterCoprocessorEnvironment> c) throws IOException {
		requirePermission("stopMaster", null, Permission.Action.ADMIN);
	}
	@Override
	public void preStopRegionServer(ObserverContext<RegionServerCoprocessorEnvironment> env) throws IOException {
		requirePermission("stop", null, Permission.Action.ADMIN);
	}
	@Override
	public void preUnassign(ObserverContext<MasterCoprocessorEnvironment> c, HRegionInfo regionInfo, boolean force) throws IOException {
		requirePermission("unassign", regionInfo.getTable().getName(), null, null, Action.ADMIN);
	}
	private String coprocessorType = "unknown";
	private static final String MASTER_COPROCESSOR_TYPE = "master";
	private static final String REGIONAL_COPROCESSOR_TYPE = "regional";
	private static final String REGIONAL_SERVER_COPROCESSOR_TYPE = "regionalServer";
	@Override
	public void start(CoprocessorEnvironment env) throws IOException {
		String appType = "unknown";

		if (env instanceof MasterCoprocessorEnvironment) {
			coprocessorType = MASTER_COPROCESSOR_TYPE;
			appType = "hbaseMaster";
		} else if (env instanceof RegionServerCoprocessorEnvironment) {
			coprocessorType = REGIONAL_SERVER_COPROCESSOR_TYPE;
			appType = "hbaseRegional";
		} else if (env instanceof RegionCoprocessorEnvironment) {
			regionEnv = (RegionCoprocessorEnvironment) env;
			coprocessorType = REGIONAL_COPROCESSOR_TYPE;
			appType = "hbseRegional";
		}

		if (superUserList == null) {
			superUserList = new ArrayList<String>();
			Configuration conf = env.getConfiguration();
			String[] users = conf.getStrings(SUPERUSER_CONFIG_PROP);
			if (users != null) {
				for (String user : users) {
					user = user.trim();
					LOG.info("Start() - Adding Super User(" + user + ")");
					superUserList.add(user);
				}
			}
		}
		// create and initialize the plugin class
		new RangerBasePlugin("hbase", appType) {}
			.init(_authorizer);
		if (LOG.isDebugEnabled()) {
			LOG.debug("Start of Coprocessor: [" + coprocessorType + "] with superUserList [" + superUserList + "]");
		}
	}
	@Override
	public void prePut(ObserverContext<RegionCoprocessorEnvironment> c, Put put, WALEdit edit, Durability durability) throws IOException {
		requirePermission("put", TablePermission.Action.WRITE, c.getEnvironment(), put.getFamilyCellMap());
	}
	
	@Override
	public void preGetOp(final ObserverContext<RegionCoprocessorEnvironment> rEnv, final Get get, final List<Cell> result) throws IOException {
		RegionCoprocessorEnvironment e = rEnv.getEnvironment();
		Map<byte[], NavigableSet<byte[]>> familyMap = get.getFamilyMap() ;

		String operation = "get";
		Filter filter = authorizeAccess(operation, Action.READ, e, familyMap);
		if (filter == null) {
			if (LOG.isDebugEnabled()) {
				LOG.debug("preGetOp: Access allowed.");
			}
		} else {
			Filter existingFilter = get.getFilter();
			Filter combinedFilter = combineFilters(filter, existingFilter);
			get.setFilter(combinedFilter);
		}
		return;
	}
	@Override
	public void preRegionOffline(ObserverContext<MasterCoprocessorEnvironment> c, HRegionInfo regionInfo) throws IOException {
	    requirePermission("regionOffline", regionInfo.getTable().getName(), null, null, Action.ADMIN);
	}
	@Override
	public void preCreateNamespace(ObserverContext<MasterCoprocessorEnvironment> ctx, NamespaceDescriptor ns) throws IOException {
		requireGlobalPermission("createNamespace", ns.getName(), Action.ADMIN);
	}
	@Override
	public void preDeleteNamespace(ObserverContext<MasterCoprocessorEnvironment> ctx, String namespace) throws IOException {
		requireGlobalPermission("deleteNamespace", namespace, Action.ADMIN);
	}
	@Override
	public void preModifyNamespace(ObserverContext<MasterCoprocessorEnvironment> ctx, NamespaceDescriptor ns) throws IOException {
		requireGlobalPermission("modifyNamespace", ns.getName(), Action.ADMIN);
	}
	@Override
	public void preGetTableDescriptors(ObserverContext<MasterCoprocessorEnvironment> ctx, List<TableName> tableNamesList,  List<HTableDescriptor> descriptors) throws IOException {
		if (tableNamesList == null || tableNamesList.isEmpty()) { // If the list is empty, this is a request for all table descriptors and requires GLOBAL ADMIN privs.
			requireGlobalPermission("getTableDescriptors", WILDCARD, Action.ADMIN);
		} else { // Otherwise, if the requestor has ADMIN or CREATE privs for all listed tables, the request can be granted.
			for (TableName tableName: tableNamesList) {
				requirePermission("getTableDescriptors", tableName.getName(), null, null, Action.CREATE);
			}
		}
	}
	@Override
	public void preMerge(ObserverContext<RegionServerCoprocessorEnvironment> ctx, HRegion regionA, HRegion regionB) throws IOException {
		requirePermission("mergeRegions", regionA.getTableDesc().getTableName().getName(), null, null, Action.ADMIN);
	}

	public void prePrepareBulkLoad(ObserverContext<RegionCoprocessorEnvironment> ctx, PrepareBulkLoadRequest request) throws IOException {
		List<byte[]> cfs = null;

		requirePermission("prePrepareBulkLoad", Permission.Action.WRITE, ctx.getEnvironment(), cfs);
	}

	public void preCleanupBulkLoad(ObserverContext<RegionCoprocessorEnvironment> ctx, CleanupBulkLoadRequest request) throws IOException {
		List<byte[]> cfs = null;

		requirePermission("preCleanupBulkLoad", Permission.Action.WRITE, ctx.getEnvironment(), cfs);
	}
	
	public static Date getUTCDate() {
		Calendar local=Calendar.getInstance();
	    int offset = local.getTimeZone().getOffset(local.getTimeInMillis());
	    GregorianCalendar utc = new GregorianCalendar(gmtTimeZone);
	    utc.setTimeInMillis(local.getTimeInMillis());
	    utc.add(Calendar.MILLISECOND, -offset);
	    return utc.getTime();
	}
	
	@Override
	public void grant(RpcController controller, AccessControlProtos.GrantRequest request, RpcCallback<AccessControlProtos.GrantResponse> done) {
		boolean isSuccess = false;

		if(UpdateRangerPoliciesOnGrantRevoke) {
			GrantRevokeData grData = null;
	
			try {
				grData = createGrantData(request);
	
				RangerAdminRESTClient xaAdmin = new RangerAdminRESTClient();
	
			    // TODO: xaAdmin.grantPrivilege(grData);
	
			    isSuccess = true;
			} catch(IOException excp) {
				LOG.warn("grant() failed", excp);
	
				ResponseConverter.setControllerException(controller, excp);
			} catch (Exception excp) {
				LOG.warn("grant() failed", excp);
	
				ResponseConverter.setControllerException(controller, new CoprocessorException(excp.getMessage()));
			} finally {
				byte[] tableName = grData == null ? null : StringUtil.getBytes(grData.getTables());
	
				// TODO - Auditing of grant-revoke to be sorted out.
//				if(accessController.isAudited(tableName)) {
//					byte[] colFamily = grData == null ? null : StringUtil.getBytes(grData.getColumnFamilies());
//					byte[] qualifier = grData == null ? null : StringUtil.getBytes(grData.getColumns());
//	
//					// Note: failed return from REST call will be logged as 'DENIED'
//					auditEvent("grant", tableName, colFamily, qualifier, null, null, getActiveUser(), isSuccess ? accessGrantedFlag : accessDeniedFlag);
//				}
			}
		}

		AccessControlProtos.GrantResponse response = isSuccess ? AccessControlProtos.GrantResponse.getDefaultInstance() : null;

		done.run(response);
	}

	@Override
	public void revoke(RpcController controller, AccessControlProtos.RevokeRequest request, RpcCallback<AccessControlProtos.RevokeResponse> done) {
		boolean isSuccess = false;

		if(UpdateRangerPoliciesOnGrantRevoke) {
			GrantRevokeData grData = null;
	
			try {
				grData = createRevokeData(request);
	
				RangerAdminRESTClient xaAdmin = new RangerAdminRESTClient();
	
			    // TODO: xaAdmin.revokePrivilege(grData);
	
			    isSuccess = true;
			} catch(IOException excp) {
				LOG.warn("revoke() failed", excp);
	
				ResponseConverter.setControllerException(controller, excp);
			} catch (Exception excp) {
				LOG.warn("revoke() failed", excp);
	
				ResponseConverter.setControllerException(controller, new CoprocessorException(excp.getMessage()));
			} finally {
				byte[] tableName = grData == null ? null : StringUtil.getBytes(grData.getTables());
	
				// TODO Audit of grant revoke to be sorted out
//				if(accessController.isAudited(tableName)) {
//					byte[] colFamily = grData == null ? null : StringUtil.getBytes(grData.getColumnFamilies());
//					byte[] qualifier = grData == null ? null : StringUtil.getBytes(grData.getColumns());
//	
//					// Note: failed return from REST call will be logged as 'DENIED'
//					auditEvent("revoke", tableName, colFamily, qualifier, null, null, getActiveUser(), isSuccess ? accessGrantedFlag : accessDeniedFlag);
//				}
			}
		}

		AccessControlProtos.RevokeResponse response = isSuccess ? AccessControlProtos.RevokeResponse.getDefaultInstance() : null;

		done.run(response);
	}

	@Override
	public void checkPermissions(RpcController controller, AccessControlProtos.CheckPermissionsRequest request, RpcCallback<AccessControlProtos.CheckPermissionsResponse> done) {
		LOG.debug("checkPermissions(): ");
	}

	@Override
	public void getUserPermissions(RpcController controller, AccessControlProtos.GetUserPermissionsRequest request, RpcCallback<AccessControlProtos.GetUserPermissionsResponse> done) {
		LOG.debug("getUserPermissions(): ");
	}

	@Override
	public Service getService() {
	    return AccessControlProtos.AccessControlService.newReflectiveService(this);
	}

	private GrantRevokeData createGrantData(AccessControlProtos.GrantRequest request) throws Exception {
		org.apache.hadoop.hbase.protobuf.generated.AccessControlProtos.UserPermission up   = request.getUserPermission();
		org.apache.hadoop.hbase.protobuf.generated.AccessControlProtos.Permission     perm = up == null ? null : up.getPermission();

		UserPermission      userPerm  = up == null ? null : ProtobufUtil.toUserPermission(up);
		Permission.Action[] actions   = userPerm == null ? null : userPerm.getActions();
		String              userName  = userPerm == null ? null : Bytes.toString(userPerm.getUser());
		String              tableName = null;
		String              colFamily = null;
		String              qualifier = null;

		if(perm == null) {
			throw new Exception("grant(): invalid data - permission is null");
		}

		if(StringUtil.isEmpty(userName)) {
			throw new Exception("grant(): invalid data - username empty");
		}

		if ((actions == null) || (actions.length == 0)) {
			throw new Exception("grant(): invalid data - no action specified");
		}

		switch(perm.getType()) {
			case Global:
				tableName = colFamily = qualifier = "*";
			break;

			case Table:
				tableName = Bytes.toString(userPerm.getTableName().getName());
				colFamily = Bytes.toString(userPerm.getFamily());
				qualifier = Bytes.toString(userPerm.getQualifier());
			break;

			case Namespace:
			default:
				LOG.warn("grant(): ignoring type '" + perm.getType().name() + "'");
			break;
		}
		
		if(StringUtil.isEmpty(tableName) && StringUtil.isEmpty(colFamily) && StringUtil.isEmpty(qualifier)) {
			throw new Exception("grant(): table/columnFamily/columnQualifier not specified");
		}

		PermMap permMap = new PermMap();

		if(userName.startsWith(GROUP_PREFIX)) {
			permMap.addGroup(userName.substring(GROUP_PREFIX.length()));
		} else {
			permMap.addUser(userName);
		}

		for (int i = 0; i < actions.length; i++) {
			switch(actions[i].code()) {
				case 'R':
				case 'W':
				case 'C':
				case 'A':
					permMap.addPerm(actions[i].name());
				break;

				default:
					LOG.warn("grant(): ignoring action '" + actions[i].name() + "' for user '" + userName + "'");
			}
		}

		User   activeUser = getActiveUser();
		String grantor    = activeUser != null ? activeUser.getShortName() : null;

		GrantRevokeData grData = new GrantRevokeData();

		grData.setHBaseData(grantor, repositoryName,  tableName,  qualifier, colFamily, permMap);

		return grData;
	}

	private GrantRevokeData createRevokeData(AccessControlProtos.RevokeRequest request) throws Exception {
		org.apache.hadoop.hbase.protobuf.generated.AccessControlProtos.UserPermission up   = request.getUserPermission();
		org.apache.hadoop.hbase.protobuf.generated.AccessControlProtos.Permission     perm = up == null ? null : up.getPermission();

		UserPermission      userPerm  = up == null ? null : ProtobufUtil.toUserPermission(up);
		String              userName  = userPerm == null ? null : Bytes.toString(userPerm.getUser());
		String              tableName = null;
		String              colFamily = null;
		String              qualifier = null;

		if(perm == null) {
			throw new Exception("revoke(): invalid data - permission is null");
		}

		if(StringUtil.isEmpty(userName)) {
			throw new Exception("revoke(): invalid data - username empty");
		}

		switch(perm.getType()) {
			case Global :
				tableName = colFamily = qualifier = "*";
			break;

			case Table :
				tableName = Bytes.toString(userPerm.getTableName().getName());
				colFamily = Bytes.toString(userPerm.getFamily());
				qualifier = Bytes.toString(userPerm.getQualifier());
			break;

			case Namespace:
			default:
				LOG.warn("revoke(): ignoring type '" + perm.getType().name() + "'");
			break;
		}
		
		if(StringUtil.isEmpty(tableName) && StringUtil.isEmpty(colFamily) && StringUtil.isEmpty(qualifier)) {
			throw new Exception("revoke(): table/columnFamily/columnQualifier not specified");
		}

		PermMap permMap = new PermMap();

		if(userName.startsWith(GROUP_PREFIX)) {
			permMap.addGroup(userName.substring(GROUP_PREFIX.length()));
		} else {
			permMap.addUser(userName);
		}

		// revoke removes all permissions
		permMap.addPerm(Permission.Action.READ.name());
		permMap.addPerm(Permission.Action.WRITE.name());
		permMap.addPerm(Permission.Action.CREATE.name());
		permMap.addPerm(Permission.Action.ADMIN.name());

		User   activeUser = getActiveUser();
		String grantor    = activeUser != null ? activeUser.getShortName() : null;

		GrantRevokeData grData = new GrantRevokeData();

		grData.setHBaseData(grantor, repositoryName,  tableName,  qualifier, colFamily, permMap);

		return grData;
	}
}
