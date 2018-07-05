import { r } from '../models'

import { getHighestRole } from '../../lib/permissions'

export async function userHasRole(userId, orgId, acceptableRoles) {
  if (r.redis) {
    // cached approach
    const userKey = `texterinfo-${userId}`
    let highestRole = await r.redis.hgetAsync(userKey, orgId)
    if (!highestRole) {
      // need to get it from db, and then cache it
      const userRoles = await r.knex('user_organization')
        .where({ user_id: userId,
                 organization_id: orgId })
        .select('role')
      if (!userRoles.length) {
        return false // who is this imposter!?
      }
      highestRole = getHighestRole(userRoles.map((r) => r.role))
      await r.redis.hsetAsync(userKey, orgId, highestRole)
    }
    return (acceptableRoles.indexOf(highestRole) >= 0)
  } else {
    // regular DB approach
    const userHasRole = await r.getCount(
      r.knex('user_organization')
        .where({ user_id: userId,
                 organization_id: orgId })
        .whereIn('role', acceptableRoles)
    )
    return userHasRole
  }
}

export async function userLoggedIn(authId) {
  const authKey = `texterauth-${authId}`

  if (r.redis) {
    const cachedAuth = await r.redis.getAsync(authKey)
    if (cachedAuth) {
      return JSON.parse(cachedAuth)
    }
  }

  const userAuth = await r.knex('user')
    .where('auth0_id', authId)
    .select('*')
    .first()

  if (r.redis && userAuth) {
    await r.redis.multi()
      .set(authKey, JSON.stringify(userAuth))
      .expire(authKey, 86400)
      .exec()
  }
  return userAuth
}

export async function updateAssignments(campaignInfo) {
  const campaignId = campaignInfo.id
  const dynamicAssignment = campaignInfo.use_dynamic_assignment
  if (r.redis) {
    const texters = await r.knex('assignment')
      .select('user_id', 'id')
      .where('campaign_id', campaignId)

    if (dynamicAssignment) {
      for (let i = 0; i < texters.length; i++) {
        // value is the actual assignments available for this campaign
        let availableAssignments = ``
        let dynamicAssignmentKey = `dynamicassignments-${campaignId}`
        await r.redis.lpush(dynamicAssignmentKey, availableAssignments)
      }
    }

    if (!dynamicAssignment) {
      for (let i = 0; i < texters.length; i++) {
        let texterId = texters[i].user_id
        let assignmentId = texters[i].id
        let texterAssignmentKey = `newassignments-${assignmentId}-${campaignId}`
        console.log('assignment key:', texterAssignmentKey);
        const assignments = await r.knex('campaign_contact')
          .where('assignment_id', assignmentId)

        await r.redis.multi()
          .set(texterAssignmentKey, JSON.stringify(assignments))
          .expire(texterAssignmentKey, 86400)
          .exec()
      }
    }
  }
}

export async function getAssignment(campaignId, assignmentId) {
  let query
  let texterAssignmentKey = `newassignments-${assignmentId}-${campaignId}`
  if (r.redis) {
    let assignments = await r.redis
      .getAsync(texterAssignmentKey)
      .then(res => {
        //returns an array
        // TODO - figure out how to handle filters now that this info is coming back as an array
        // versus a knex promise
        return JSON.parse(res)
      })
    return assignments
  } else {
    query = r.knex('campaign_contact').where('assignment_id', assignment.id)
    return query
  }
}

export async function getAssignmentByMessageStatus(query, messageStatus) {
  let result
  if (r.redis) {
    console.log('query:', query);
    console.log('message status:', messageStatus);
  } else {
    result = query.where('message_status', messageStatus)
    return result
  }
}

// export async function getAssignmentFilter(command, assignmentFilter, offsets) {
//   console.log('command:', command);
//   console.log('filter:', assignmentFilter);
//   console.log('offsets:', offsets);
// query = query.whereIn('timezone_offset', invalidOffsets)
//   if (command === 'whereIn' && assignmentFilter === 'timezone_offset') {
//
//   }
// }
